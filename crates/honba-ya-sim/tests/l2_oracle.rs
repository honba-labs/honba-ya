//! Differential oracle for the L2 simulator (ADR 0030 section 5, roadmap E3-S1d).
//!
//! A deliberately naive reference matcher ([`Naive`]) is written from the ADR 0030 section 3
//! rules and the `l2_sim.json` fixture only, with none of `L2Sim`'s structures: orders live on an
//! append-only tape that is rescanned in full on every event (O(n^2) is fine), each order carries
//! a status flag instead of being removed from a queue, and the book is a plain 4-tuple. Seeded
//! random event streams are replayed through both and compared after every step: the event queue
//! (fills, rejections, cancels, expiries, with timestamps), cash, fees, traded notional,
//! positions and the working-order list. A failure names the seed and the first step that
//! diverges.
//!
//! Shared on purpose, and only these: the money-unit conversion `Money::mul_qty` (notional in
//! minor units) and the vectors' test cost formula (`flat + (notional * bps + 5000) / 10000`),
//! both configuration of the world rather than matching logic.
//!
//! Where ADR 0030 is silent, the fixture (hand-derived, authoritative) and the `l2` module docs
//! decide; each such decision is marked `AMBIGUITY` below:
//!
//! 1. A quote re-matches every unarmed resting order of its instrument in submission order,
//!    against a touch that earlier orders of the same quote have already drawn down.
//! 2. A print's size is shared FIFO by the resting limits it trades through; stops it triggers
//!    only see the book afterwards and do not take part in the same print's trade-through.
//! 3. IOC / FOK are judged exactly once, against the book at submit (or trigger) time.
//! 4. A Day order expires at the first close at or after its submit time, observed on the next
//!    event with `ts >= close` (stamped with the close instant); an explicit `session_close`
//!    expires every Day order, armed stops included, stamped with its own time.
//! 5. A funding cut or a long-only cap fills what is possible and then rejects the remainder
//!    (`insufficient_funds` / `no_position`), even when nothing could be filled.
//! 6. A cancel is stamped with the caller's `now` for both `CancelRequested` and `Cancelled`
//!    and does not move event time; cancelling an unknown or finished id is a silent no-op.
//! 7. Trailing stops are refused (`unsupported_order_type`) before any validity check.

use honba_engine::ExecutionEngine;
use honba_entities::{Currency, ExecutionEvent, Money};
use honba_messages::{
    AggressorSide, Exchange, InstrumentId, Order, OrderId, OrderSide, OrderType, QuoteTick,
    TimeInForce, TradeId, TradeTick, UnixNanos,
};
use honba_sim::L2Sim;

const EPS: f64 = 1e-9;
const NS: u64 = 1_000_000_000;
const DAY_NS: u64 = 86_400 * NS;
const CLOSE_NS: u64 = 86_340 * NS;
const SYMBOLS: [&str; 2] = ["AAA", "BBB"];
const TICK: f64 = 0.05;

// ---- deterministic RNG (splitmix64; no dependency, stable across versions) ----------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// Inclusive range.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

// ---- stream model --------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Market,
    Limit,
    StopMarket,
    StopLimit,
    Trailing,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Tif {
    Day,
    Gtc,
    Ioc,
    Fok,
}

#[derive(Clone, Debug)]
struct Spec {
    id: String,
    sym: &'static str,
    buy: bool,
    kind: Kind,
    qty: f64,
    price: Option<f64>,
    trigger: Option<f64>,
    tif: Tif,
    ts: u64,
}

#[derive(Clone, Debug)]
enum Step {
    Quote {
        sym: &'static str,
        ts: u64,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
    },
    Trade {
        sym: &'static str,
        ts: u64,
        price: f64,
        size: f64,
    },
    Submit(Spec),
    Cancel {
        id: String,
        now: u64,
    },
    Advance(u64),
    SessionClose(u64),
}

#[derive(Clone, Debug)]
struct Config {
    cash: i64,
    long_only: bool,
    lots: Vec<(&'static str, f64)>,
    positions: Vec<(&'static str, f64)>,
    flat_buy: i64,
    flat_sell: i64,
    bps_buy: i64,
    bps_sell: i64,
}

#[derive(Clone, Debug)]
struct Case {
    cfg: Config,
    steps: Vec<Step>,
}

fn notional_minor(qty: f64, price: f64) -> i64 {
    Money::mul_qty(qty, price, Currency::Inr).unwrap().minor()
}

fn cost_minor(cfg: &Config, buy: bool, qty: f64, price: f64) -> i64 {
    let n = notional_minor(qty, price);
    let (flat, bps) = if buy {
        (cfg.flat_buy, cfg.bps_buy)
    } else {
        (cfg.flat_sell, cfg.bps_sell)
    };
    flat + (n * bps + 5000).div_euclid(10_000)
}

fn px(ticks: i64) -> f64 {
    ticks as f64 * TICK
}

/// Builds the stream for `seed`; bounded to at most 100 steps.
fn generate(seed: u64) -> Case {
    let mut r = Rng::new(seed);
    r.next(); // decorrelate adjacent seeds
    let cash = [150_000, 1_000_000, 20_000_000, 1_000_000_000][r.below(4) as usize];
    let lot_choices = [1.0, 5.0, 10.0];
    let cfg = Config {
        cash,
        long_only: r.chance(70),
        lots: SYMBOLS
            .iter()
            .map(|s| (*s, lot_choices[r.below(3) as usize]))
            .collect(),
        positions: SYMBOLS
            .iter()
            .filter_map(|s| {
                if r.chance(50) {
                    Some((*s, r.range(1, 60) as f64))
                } else {
                    None
                }
            })
            .collect(),
        flat_buy: r.range(0, 30),
        flat_sell: r.range(0, 30),
        bps_buy: r.range(0, 20),
        bps_sell: r.range(0, 20),
    };
    let mut mid = [2000_i64 + r.range(-50, 50), 800 + r.range(-30, 30)];
    // Start up to 250 s before the 23:59:00 UTC default close so Day expiry is reachable.
    let mut ts = CLOSE_NS - r.below(250) * NS;
    let mut issued: Vec<String> = Vec::new();
    let mut steps = Vec::new();
    let n = r.range(30, 100);
    for _ in 0..n {
        ts += if r.chance(20) {
            0
        } else {
            r.below(12_000) * 1_000_000
        };
        let s = r.below(2) as usize;
        let sym = SYMBOLS[s];
        let roll = r.below(100);
        if roll < 25 {
            mid[s] = (mid[s] + r.range(-3, 3)).max(20);
            let spread = r.range(0, 4);
            let bid_t = mid[s] - spread / 2;
            let size = |r: &mut Rng| {
                if r.chance(8) {
                    0.0
                } else {
                    r.range(1, 40) as f64
                }
            };
            steps.push(Step::Quote {
                sym,
                ts,
                bid: px(bid_t),
                ask: px(bid_t + spread),
                bid_size: size(&mut r),
                ask_size: size(&mut r),
            });
        } else if roll < 45 {
            steps.push(Step::Trade {
                sym,
                ts,
                price: px((mid[s] + r.range(-4, 4)).max(1)),
                size: r.range(1, 30) as f64,
            });
        } else if roll < 78 {
            let k = r.below(100);
            let kind = match k {
                0..=19 => Kind::Market,
                20..=57 => Kind::Limit,
                58..=69 => Kind::StopMarket,
                70..=81 => Kind::StopLimit,
                82..=84 => Kind::Trailing,
                _ => Kind::Limit,
            };
            let t = r.below(100);
            let tif = match t {
                0..=29 => Tif::Day,
                30..=64 => Tif::Gtc,
                65..=81 => Tif::Ioc,
                _ => Tif::Fok,
            };
            let id = format!("O{}", issued.len());
            issued.push(id.clone());
            let needs_limit = matches!(kind, Kind::Limit | Kind::StopLimit);
            let needs_stop = matches!(kind, Kind::StopMarket | Kind::StopLimit | Kind::Trailing);
            let price = needs_limit.then(|| {
                if r.chance(5) {
                    None
                } else {
                    Some(px((mid[s] + r.range(-4, 4)).max(1)))
                }
            });
            steps.push(Step::Submit(Spec {
                id,
                sym,
                buy: r.chance(50),
                kind,
                // Order::new debug-asserts qty > 0, so a bad quantity cannot reach the sim; a Limit
                // without a price (below) is the reachable `invalid_order`.
                qty: r.range(1, 40) as f64,
                price: price.flatten(),
                trigger: needs_stop.then(|| px((mid[s] + r.range(-5, 5)).max(1))),
                tif,
                ts: if r.chance(10) {
                    ts.saturating_sub(r.below(5) * NS)
                } else {
                    ts
                },
            }));
        } else if roll < 86 {
            let id = if issued.is_empty() || r.chance(10) {
                "NOPE".to_string()
            } else {
                issued[r.below(issued.len() as u64) as usize].clone()
            };
            steps.push(Step::Cancel { id, now: ts });
        } else if roll < 92 {
            ts += r.below(60) * NS;
            steps.push(Step::Advance(ts));
        } else if roll < 95 {
            steps.push(Step::SessionClose(ts));
        } else {
            steps.push(Step::Advance(ts));
        }
    }
    Case { cfg, steps }
}

// ---- normalised outputs --------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Ev {
    Fill {
        id: String,
        sym: String,
        buy: bool,
        qty: f64,
        px: f64,
        ts: u64,
        cost: i64,
        cum: f64,
        complete: bool,
    },
    Reject {
        id: String,
        qty: f64,
        reason: String,
        ts: u64,
    },
    CancelReq {
        id: String,
        ts: u64,
    },
    Cancelled {
        id: String,
        qty: f64,
        ts: u64,
    },
    Expired {
        id: String,
        qty: f64,
        ts: u64,
    },
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPS
}

fn ev_eq(a: &Ev, b: &Ev) -> bool {
    use Ev::*;
    match (a, b) {
        (
            Fill {
                id: i1,
                sym: s1,
                buy: b1,
                qty: q1,
                px: p1,
                ts: t1,
                cost: c1,
                cum: m1,
                complete: x1,
            },
            Fill {
                id: i2,
                sym: s2,
                buy: b2,
                qty: q2,
                px: p2,
                ts: t2,
                cost: c2,
                cum: m2,
                complete: x2,
            },
        ) => {
            i1 == i2
                && s1 == s2
                && b1 == b2
                && close(*q1, *q2)
                && close(*p1, *p2)
                && t1 == t2
                && c1 == c2
                && close(*m1, *m2)
                && x1 == x2
        }
        (
            Reject {
                id: i1,
                qty: q1,
                reason: r1,
                ts: t1,
            },
            Reject {
                id: i2,
                qty: q2,
                reason: r2,
                ts: t2,
            },
        ) => i1 == i2 && close(*q1, *q2) && r1 == r2 && t1 == t2,
        (CancelReq { id: i1, ts: t1 }, CancelReq { id: i2, ts: t2 }) => i1 == i2 && t1 == t2,
        (
            Cancelled {
                id: i1,
                qty: q1,
                ts: t1,
            },
            Cancelled {
                id: i2,
                qty: q2,
                ts: t2,
            },
        )
        | (
            Expired {
                id: i1,
                qty: q1,
                ts: t1,
            },
            Expired {
                id: i2,
                qty: q2,
                ts: t2,
            },
        ) => i1 == i2 && close(*q1, *q2) && t1 == t2,
        _ => false,
    }
}

#[derive(Clone, Debug)]
struct State {
    cash: i64,
    fees: i64,
    notional: i64,
    positions: Vec<(String, f64)>,
    working: Vec<String>,
}

fn state_eq(a: &State, b: &State) -> bool {
    a.cash == b.cash
        && a.fees == b.fees
        && a.notional == b.notional
        && a.working == b.working
        && a.positions.len() == b.positions.len()
        && a.positions
            .iter()
            .zip(&b.positions)
            .all(|(x, y)| x.0 == y.0 && close(x.1, y.1))
}

type Trace = Vec<(Vec<Ev>, State)>;

// ---- the naive reference matcher -----------------------------------------------------------

struct Rec {
    spec: Spec,
    rem: f64,
    filled: f64,
    armed: bool,
    live: bool,
    limit: Option<f64>,
    expire_at: Option<u64>,
}

/// The oracle. `strict_through` is a deliberate mutation switch for the self-test only.
struct Naive {
    cfg: Config,
    strict_through: bool,
    cash: i64,
    fees: i64,
    notional: i64,
    pos: Vec<(String, f64)>,
    /// (symbol, bid, ask, bid_size, ask_size)
    books: Vec<(String, f64, f64, f64, f64)>,
    now: u64,
    tape: Vec<Rec>,
    out: Vec<Ev>,
}

impl Naive {
    fn new(cfg: &Config, strict_through: bool) -> Self {
        Naive {
            cfg: cfg.clone(),
            strict_through,
            cash: cfg.cash,
            fees: 0,
            notional: 0,
            pos: cfg
                .positions
                .iter()
                .map(|(s, q)| (s.to_string(), *q))
                .collect(),
            books: Vec::new(),
            now: 0,
            tape: Vec::new(),
            out: Vec::new(),
        }
    }

    fn position(&self, sym: &str) -> f64 {
        self.pos
            .iter()
            .filter(|p| p.0 == sym)
            .map(|p| p.1)
            .sum::<f64>()
    }

    fn lot(&self, sym: &str) -> f64 {
        self.cfg
            .lots
            .iter()
            .find(|l| l.0 == sym)
            .map_or(1.0, |l| l.1)
    }

    fn advance(&mut self, ts: u64) {
        for i in 0..self.tape.len() {
            let r = &self.tape[i];
            if let (true, Some(at)) = (r.live, r.expire_at) {
                if at <= ts {
                    self.expire(i, at);
                }
            }
        }
        self.now = self.now.max(ts);
    }

    fn expire(&mut self, i: usize, ts: u64) {
        self.tape[i].live = false;
        self.out.push(Ev::Expired {
            id: self.tape[i].spec.id.clone(),
            qty: self.tape[i].rem,
            ts,
        });
    }

    fn reject(&mut self, i: usize, reason: &str, ts: u64) {
        self.tape[i].live = false;
        self.out.push(Ev::Reject {
            id: self.tape[i].spec.id.clone(),
            qty: self.tape[i].rem,
            reason: reason.to_string(),
            ts,
        });
    }

    fn submit(&mut self, spec: &Spec) {
        let ts = spec.ts.max(self.now);
        self.advance(ts);
        let limit = match spec.kind {
            Kind::Limit | Kind::StopLimit => spec.price,
            _ => None,
        };
        let mut rec = Rec {
            spec: spec.clone(),
            rem: spec.qty,
            filled: 0.0,
            armed: false,
            live: true,
            limit,
            expire_at: None,
        };
        let usable = |p: Option<f64>| p.is_some_and(|p| p.is_finite() && p > 0.0);
        let bad = !(spec.qty.is_finite() && spec.qty > 0.0)
            || (matches!(spec.kind, Kind::Limit | Kind::StopLimit) && !usable(spec.price))
            || (matches!(spec.kind, Kind::StopMarket | Kind::StopLimit) && !usable(spec.trigger));
        let reason = if spec.kind == Kind::Trailing {
            Some("unsupported_order_type")
        } else if bad {
            Some("invalid_order")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.tape.push(rec);
            let i = self.tape.len() - 1;
            self.reject(i, reason, ts);
            return;
        }
        rec.armed = matches!(spec.kind, Kind::StopMarket | Kind::StopLimit);
        if spec.tif == Tif::Day {
            let today_close = ts / DAY_NS * DAY_NS + CLOSE_NS;
            rec.expire_at = Some(if today_close < ts {
                today_close + DAY_NS
            } else {
                today_close
            });
        }
        let armed = rec.armed;
        self.tape.push(rec);
        if !armed {
            let i = self.tape.len() - 1;
            self.aggress(i, ts, true);
        }
    }

    fn cancel(&mut self, id: &str, now: u64) {
        for i in 0..self.tape.len() {
            if self.tape[i].live && self.tape[i].spec.id == id {
                self.tape[i].live = false;
                self.out.push(Ev::CancelReq {
                    id: id.to_string(),
                    ts: now,
                });
                self.out.push(Ev::Cancelled {
                    id: id.to_string(),
                    qty: self.tape[i].rem,
                    ts: now,
                });
                return;
            }
        }
    }

    fn session_close(&mut self, ts: u64) {
        self.now = self.now.max(ts);
        for i in 0..self.tape.len() {
            if self.tape[i].live && self.tape[i].spec.tif == Tif::Day {
                self.expire(i, ts);
            }
        }
    }

    fn quote(&mut self, sym: &str, ts: u64, bid: f64, ask: f64, bs: f64, asz: f64) {
        self.advance(ts);
        self.books.retain(|b| b.0 != sym);
        self.books.push((sym.to_string(), bid, ask, bs, asz));
        // AMBIGUITY 1: every unarmed resting order of the instrument, submission order.
        for i in 0..self.tape.len() {
            let r = &self.tape[i];
            if r.live && !r.armed && r.spec.sym == sym {
                self.aggress(i, ts, false);
            }
        }
    }

    fn trade(&mut self, sym: &str, ts: u64, price: f64, size: f64) {
        self.advance(ts);
        let mut left = size;
        // AMBIGUITY 2: first the resting limits, FIFO, sharing the print's size...
        for i in 0..self.tape.len() {
            let r = &self.tape[i];
            if !(r.live && !r.armed && r.spec.sym == sym) {
                continue;
            }
            let Some(l) = r.limit else { continue };
            let through = match (r.spec.buy, self.strict_through) {
                (true, false) => price <= l,
                (true, true) => price < l,
                (false, false) => price >= l,
                (false, true) => price > l,
            };
            if through && left > EPS {
                let (filled, _) = self.execute(i, left, l, ts, true);
                left -= filled;
            }
        }
        // ...then the stops it crosses, which act on the book as it now stands.
        for i in 0..self.tape.len() {
            let r = &self.tape[i];
            if !(r.live && r.armed && r.spec.sym == sym) {
                continue;
            }
            let trig = r.spec.trigger.unwrap();
            let hit = if r.spec.buy {
                price >= trig
            } else {
                price <= trig
            };
            if hit {
                self.tape[i].armed = false;
                self.aggress(i, ts, true);
            }
        }
    }

    /// The (price, size) the order of `i` would trade against, honouring its limit.
    fn touch(&self, i: usize) -> Option<(f64, f64)> {
        let r = &self.tape[i];
        let b = self.books.iter().find(|b| b.0 == r.spec.sym)?;
        let (p, s) = if r.spec.buy { (b.2, b.4) } else { (b.1, b.3) };
        if !(p.is_finite() && p > 0.0 && s > 0.0) {
            return None;
        }
        match r.limit {
            Some(l) if r.spec.buy && p > l => None,
            Some(l) if !r.spec.buy && p < l => None,
            _ => Some((p, s)),
        }
    }

    /// Returns whether the order left the working set. AMBIGUITY 3: IOC/FOK only when `first`.
    fn aggress(&mut self, i: usize, ts: u64, first: bool) -> bool {
        let touch = self.touch(i);
        let (tif, rem) = (self.tape[i].spec.tif, self.tape[i].rem);
        if first && tif == Tif::Fok && touch.map_or(true, |(_, s)| s + EPS < rem) {
            self.reject(i, "fok_unfillable", ts);
            return true;
        }
        if let Some((p, s)) = touch {
            let (filled, gone) = self.execute(i, s.min(rem), p, ts, false);
            let sym = self.tape[i].spec.sym;
            let buy = self.tape[i].spec.buy;
            for b in self.books.iter_mut().filter(|b| b.0 == sym) {
                if buy {
                    b.4 = (b.4 - filled).max(0.0);
                } else {
                    b.3 = (b.3 - filled).max(0.0);
                }
            }
            if gone {
                return true;
            }
        }
        if first && tif == Tif::Ioc {
            self.tape[i].live = false;
            self.out.push(Ev::Cancelled {
                id: self.tape[i].spec.id.clone(),
                qty: self.tape[i].rem,
                ts,
            });
            return true;
        }
        false
    }

    fn buy_cost(&self, qty: f64, p: f64) -> i64 {
        notional_minor(qty, p) + cost_minor(&self.cfg, true, qty, p)
    }

    /// AMBIGUITY 5. Fills up to `avail` at `base`; returns (filled, left the working set).
    fn execute(&mut self, i: usize, avail: f64, base: f64, ts: u64, _passive: bool) -> (f64, bool) {
        let (sym, buy, limit) = (
            self.tape[i].spec.sym,
            self.tape[i].spec.buy,
            self.tape[i].limit,
        );
        let mut qty = avail.min(self.tape[i].rem);
        let mut cut: Option<&str> = None;
        if self.cfg.long_only && !buy {
            let capped = qty.min(self.position(sym)).max(0.0);
            if capped < qty {
                cut = Some("no_position");
                qty = capped;
            }
        }
        let mut p = base; // no slippage model in the oracle config
        if let Some(l) = limit {
            p = if buy { p.min(l) } else { p.max(l) };
        }
        if buy && qty > 0.0 && self.buy_cost(qty, p) > self.cash {
            let lot = self.lot(sym);
            let mut k = (qty / lot + EPS).floor() as i64;
            while k > 0 && self.buy_cost(k as f64 * lot, p) > self.cash {
                k -= 1;
            }
            qty = k as f64 * lot;
            cut = Some("insufficient_funds");
        }
        if qty > 0.0 {
            let n = notional_minor(qty, p);
            let c = cost_minor(&self.cfg, buy, qty, p);
            self.cash += if buy { -(n + c) } else { n - c };
            self.fees += c;
            self.notional += n;
            let r = &mut self.tape[i];
            r.rem -= qty;
            r.filled += qty;
            let (cum, complete) = (r.filled, r.rem <= EPS);
            let held = self.position(sym) + if buy { qty } else { -qty };
            self.pos.retain(|x| x.0 != sym);
            if held.abs() > EPS {
                self.pos.push((sym.to_string(), held));
            }
            self.out.push(Ev::Fill {
                id: self.tape[i].spec.id.clone(),
                sym: sym.to_string(),
                buy,
                qty,
                px: p,
                ts,
                cost: c,
                cum,
                complete,
            });
        }
        if let Some(reason) = cut {
            self.reject(i, reason, ts);
            return (qty, true);
        }
        if self.tape[i].rem <= EPS {
            self.tape[i].live = false;
            return (qty, true);
        }
        (qty, false)
    }

    fn state(&self) -> State {
        let mut positions = self.pos.clone();
        positions.sort_by(|a, b| a.0.cmp(&b.0));
        State {
            cash: self.cash,
            fees: self.fees,
            notional: self.notional,
            positions,
            working: self
                .tape
                .iter()
                .filter(|r| r.live)
                .map(|r| r.spec.id.clone())
                .collect(),
        }
    }
}

// ---- drivers -------------------------------------------------------------------------------

fn iid(sym: &str) -> InstrumentId {
    InstrumentId::new(sym, Exchange::new("NSE"))
}

fn naive_trace(case: &Case, strict_through: bool) -> Trace {
    let mut n = Naive::new(&case.cfg, strict_through);
    case.steps
        .iter()
        .map(|step| {
            match step {
                Step::Quote {
                    sym,
                    ts,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                } => n.quote(sym, *ts, *bid, *ask, *bid_size, *ask_size),
                Step::Trade {
                    sym,
                    ts,
                    price,
                    size,
                } => n.trade(sym, *ts, *price, *size),
                Step::Submit(spec) => n.submit(spec),
                Step::Cancel { id, now } => n.cancel(id, *now),
                Step::Advance(ts) => n.advance(*ts),
                Step::SessionClose(ts) => n.session_close(*ts),
            }
            (std::mem::take(&mut n.out), n.state())
        })
        .collect()
}

fn build_sim(cfg: &Config, long_only: bool) -> L2Sim {
    let c = cfg.clone();
    let mut sim = L2Sim::new(Money::new(cfg.cash, Currency::Inr))
        .unwrap()
        .with_long_only(long_only)
        .with_costs(Box::new(move |side, qty, price| {
            Ok(Money::new(
                cost_minor(&c, side == OrderSide::Buy, qty, price),
                Currency::Inr,
            ))
        }));
    for (s, lot) in &cfg.lots {
        sim.set_lot_size(&iid(s), *lot).unwrap();
    }
    for (s, q) in &cfg.positions {
        sim.set_position(&iid(s), *q).unwrap();
    }
    sim
}

fn to_ev(e: ExecutionEvent) -> Ev {
    match e {
        ExecutionEvent::Fill {
            trade,
            cum_qty,
            complete,
            ..
        } => Ev::Fill {
            id: trade.order_id().as_str().to_string(),
            sym: trade.instrument_id().symbol().to_string(),
            buy: trade.side() == OrderSide::Buy,
            qty: trade.quantity(),
            px: trade.price(),
            ts: trade.ts_event().as_u64(),
            cost: trade.costs().minor(),
            cum: cum_qty,
            complete,
        },
        ExecutionEvent::Rejected {
            order_id,
            quantity,
            reason,
            ts,
            ..
        } => Ev::Reject {
            id: order_id.as_str().to_string(),
            qty: quantity,
            reason,
            ts: ts.as_u64(),
        },
        ExecutionEvent::CancelRequested { order_id, ts } => Ev::CancelReq {
            id: order_id.as_str().to_string(),
            ts: ts.as_u64(),
        },
        ExecutionEvent::Cancelled {
            order_id,
            quantity,
            ts,
            ..
        } => Ev::Cancelled {
            id: order_id.as_str().to_string(),
            qty: quantity,
            ts: ts.as_u64(),
        },
        ExecutionEvent::Expired {
            order_id,
            quantity,
            ts,
            ..
        } => Ev::Expired {
            id: order_id.as_str().to_string(),
            qty: quantity,
            ts: ts.as_u64(),
        },
        other => panic!("unexpected event {other:?}"),
    }
}

fn to_order(s: &Spec) -> Order {
    let order_type = match s.kind {
        Kind::Market => OrderType::Market,
        Kind::Limit => OrderType::Limit,
        Kind::StopMarket => OrderType::StopMarket,
        Kind::StopLimit => OrderType::StopLimit,
        Kind::Trailing => OrderType::TrailingStop,
    };
    let tif = match s.tif {
        Tif::Day => TimeInForce::Day,
        Tif::Gtc => TimeInForce::Gtc,
        Tif::Ioc => TimeInForce::Ioc,
        Tif::Fok => TimeInForce::Fok,
    };
    let t = UnixNanos::from_u64(s.ts);
    let o = Order::new(
        OrderId::new(&s.id),
        iid(s.sym),
        if s.buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type,
        s.qty,
        s.price,
        tif,
        t,
        t,
    );
    match s.trigger {
        Some(trig) => o.with_trigger_price(trig),
        None => o,
    }
}

fn sim_trace(case: &Case, long_only: bool) -> Trace {
    let mut sim = build_sim(&case.cfg, long_only);
    case.steps
        .iter()
        .map(|step| {
            match step {
                Step::Quote {
                    sym,
                    ts,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                } => {
                    let t = UnixNanos::from_u64(*ts);
                    sim.on_quote(&QuoteTick::new(
                        iid(sym),
                        *bid,
                        *ask,
                        *bid_size,
                        *ask_size,
                        t,
                        t,
                    ))
                    .unwrap();
                }
                Step::Trade {
                    sym,
                    ts,
                    price,
                    size,
                } => {
                    let t = UnixNanos::from_u64(*ts);
                    sim.on_trade(&TradeTick::new(
                        iid(sym),
                        *price,
                        *size,
                        AggressorSide::Buyer,
                        TradeId::new(format!("T-{ts}")),
                        t,
                        t,
                    ))
                    .unwrap();
                }
                Step::Submit(spec) => ExecutionEngine::submit(&mut sim, to_order(spec)).unwrap(),
                Step::Cancel { id, now } => {
                    ExecutionEngine::cancel(&mut sim, id, UnixNanos::from_u64(*now)).unwrap()
                }
                Step::Advance(ts) => sim.advance_to(UnixNanos::from_u64(*ts)).unwrap(),
                Step::SessionClose(ts) => sim.on_session_close(UnixNanos::from_u64(*ts)).unwrap(),
            }
            let events = sim
                .drain_events()
                .unwrap()
                .into_iter()
                .map(to_ev)
                .collect::<Vec<_>>();
            let mut positions: Vec<(String, f64)> = sim
                .positions()
                .iter()
                .map(|(i, q)| (i.symbol().to_string(), *q))
                .collect();
            positions.sort_by(|a, b| a.0.cmp(&b.0));
            let state = State {
                cash: sim.cash().minor(),
                fees: sim.fees().minor(),
                notional: sim.traded_notional().minor(),
                positions,
                working: sim.working_orders(),
            };
            (events, state)
        })
        .collect()
}

/// `Some(report)` naming the seed and the first diverging step.
fn diff(seed: u64, case: &Case, want: &Trace, got: &Trace) -> Option<String> {
    for (i, (w, g)) in want.iter().zip(got).enumerate() {
        let events_ok = w.0.len() == g.0.len() && w.0.iter().zip(&g.0).all(|(a, b)| ev_eq(a, b));
        if !events_ok || !state_eq(&w.1, &g.1) {
            return Some(format!(
                "oracle divergence: seed {seed}, first at step {i} of {}\n step: {:?}\n config: {:?}\n \
                 oracle events: {:?}\n sim events:    {:?}\n oracle state: {:?}\n sim state:    {:?}",
                case.steps.len(),
                case.steps[i],
                case.cfg,
                w.0,
                g.0,
                w.1,
                g.1
            ));
        }
    }
    None
}

fn trace_eq(a: &Trace, b: &Trace) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.0.len() == y.0.len()
                && x.0.iter().zip(&y.0).all(|(p, q)| ev_eq(p, q))
                && state_eq(&x.1, &y.1)
        })
}

const SEEDS: u64 = 600;

#[test]
fn oracle_agrees_seeded_streams() {
    let mut seen = std::collections::BTreeMap::<String, u64>::new();
    for seed in 0..SEEDS {
        let case = generate(seed);
        assert!(case.steps.len() <= 100);
        let want = naive_trace(&case, false);
        let got = sim_trace(&case, case.cfg.long_only);
        if let Some(report) = diff(seed, &case, &want, &got) {
            panic!("{report}");
        }
        for (events, _) in &want {
            for e in events {
                let key = match e {
                    Ev::Fill { .. } => "fill".to_string(),
                    Ev::Reject { reason, .. } => format!("reject:{reason}"),
                    Ev::CancelReq { .. } => "cancel_requested".to_string(),
                    Ev::Cancelled { .. } => "cancelled".to_string(),
                    Ev::Expired { .. } => "expired".to_string(),
                };
                *seen.entry(key).or_default() += 1;
            }
        }
    }
    // The streams must exercise every outcome, or agreement would be vacuous.
    for key in [
        "fill",
        "reject:fok_unfillable",
        "reject:insufficient_funds",
        "reject:no_position",
        "reject:invalid_order",
        "reject:unsupported_order_type",
        "cancel_requested",
        "cancelled",
        "expired",
    ] {
        assert!(
            seen.get(key).copied().unwrap_or(0) > 20,
            "weak coverage of {key}: {seen:?}"
        );
    }
}

#[test]
fn oracle_and_generator_are_deterministic() {
    for seed in [0_u64, 1, 7, 42, 599] {
        let (a, b) = (generate(seed), generate(seed));
        assert_eq!(format!("{a:?}"), format!("{b:?}"), "seed {seed}: generator");
        assert!(trace_eq(&naive_trace(&a, false), &naive_trace(&b, false)));
        assert!(trace_eq(
            &sim_trace(&a, a.cfg.long_only),
            &sim_trace(&b, b.cfg.long_only)
        ));
    }
    assert_ne!(
        format!("{:?}", generate(1).steps),
        format!("{:?}", generate(2).steps),
        "different seeds must give different streams"
    );
}

#[test]
fn oracle_detects_a_mutated_reference() {
    // Trade-through made strict (price < limit instead of <=) must be caught.
    let caught = (0..SEEDS).any(|seed| {
        let case = generate(seed);
        diff(
            seed,
            &case,
            &naive_trace(&case, true),
            &sim_trace(&case, case.cfg.long_only),
        )
        .is_some()
    });
    assert!(caught, "a strict trade-through mutation went undetected");
}

#[test]
fn oracle_detects_a_misconfigured_sim() {
    // A sim whose long-only flag disagrees with the oracle's config must be caught.
    let caught = (0..SEEDS).any(|seed| {
        let case = generate(seed);
        diff(
            seed,
            &case,
            &naive_trace(&case, false),
            &sim_trace(&case, !case.cfg.long_only),
        )
        .is_some()
    });
    assert!(caught, "a mis-configured sim went undetected");
}
