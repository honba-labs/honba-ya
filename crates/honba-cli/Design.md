# Honba CLI: command surface and screener design

Status: proposed. Scope: the `honba` command-line interface, starting with the `screener` app.

## 1. Goals

1. Every option and filter maps directly onto a domain/wire type, so the CLI adds no semantics of its own.
2. Filters read like English, including Indian and international money units (`Lk`, `Cr`, `Mn`, `Bn`, `Tn`).
3. One uniform command shape across apps, usable by humans, scripts and LLM agents.
4. Deterministic and reproducible: a command can be reduced to a JSON request and replayed.

Non-goals: new broker connectivity, a general-purpose query language, FX/multi-currency support.

## 2. Where this lives

Two entry points exist today:

| Entry | Stack | Today |
|---|---|---|
| `honba` (`honba.cli.main:app`) | Python, typer | `indicators`, `schema`; `backtest`, `data`, `optimize`, `research`, `strategy` are empty stubs |
| `honba-cli` crate | Rust, clap | `backtest`, `calendars`, `data` |

The screener is **Python-first** (workspace principle). It is added to the Python `honba` entry as `honba screener`.
The Rust binary is unchanged. If profiling later shows that evaluation over the full universe needs Rust, the
evaluator moves behind the same Python API (exposed through `honba._honba`, with a `.pyi` stub). The CLI surface in
this document does not change when that happens.

## 3. Command shape

```
honba <app> <sub-command> [<sub-sub-command>] [options] [filters...]
```

- Options come first. Everything after the last option is the filter text (trailing positional words).
- Parsing stops at the first non-option word (`allow_interspersed_args=False` in click/typer). `--` forces the
  remainder to be filter text.
- Filter words may be passed as separate shell words or as one quoted string. Both are joined with single spaces
  before parsing.

### 3.1 Screener commands

| Command | Purpose |
|---|---|
| `honba screener scan [options] <filters>` | Run a scan and render results |
| `honba screener explain [options] <filters>` | Parse and validate; print the parse tree and the resolved request; do not run |
| `honba screener metrics list\|show` | Browse the metric catalog (`MetricDefinition`) |
| `honba screener presets list\|show` | Browse named criteria such as `52 week low` |
| `honba screener save\|run` | Persist a request as `configs/screener/*.toml` and rerun it |
| `honba screener ask [options] <free text>` | Translate free text to a validated request with an LLM, show it, then run (section 13) |
| `honba data coverage\|gaps\|fetch` | Inspect what the store holds, what is missing, and fill gaps (section 12) |
| `honba ai knowledge export\|check`, `honba ai eval screener` | Generate and drift-check the LLM knowledge pack; run the NL-to-query eval (section 13) |

### 3.2 Options of `scan`

Each option maps to a field of `ScreenerScanRequest` (`honba.entities.screener`; Rust mirror in `honba-entities`).
Field names below are the **wire** names (camelCase, as the screener models serialise them); see section 14.

| Option | Request field | Notes |
|---|---|---|
| `--market` | `market` | e.g. `india` |
| `--type` (repeatable) | `types` | default `EQUITY`; accepts the UI names `stocks`, `etf`, `mf`, `bonds`, `index`, `ipo` and normalises them to the wire enum (14.4) |
| `--universe` | symbol filter | resolved from `honba.india.universes`, e.g. `nifty50` |
| `--all-listings` | `primaryOnly=false` | default is primary listing only |
| `--timescale`, `-t` | default `timeframe` | `1d`, `1D`, `daily`, `D1` all normalise to `Timeframe.D1` |
| `--period` | default `period` | `snapshot`, `ttm`, `fy`, `fq`, `h1`, `current` |
| `--columns` | `columns[]` | list of `MetricKeySpec`, comma separated, accepts `key@timeframe` |
| `--column-set` | `columnSet` | named column set |
| `--sort 'key[@tf]:asc\|desc'` | `sort` | `ScreenerSortSpec` |
| `--limit`, `--offset` | `range` | default `(0, 50)` |
| `--request FILE\|-` | whole request | bypasses filter text; mutually exclusive with filters |
| `--print-request` | n/a | emit the resolved request as JSON and exit |
| `--format` | n/a | `table` (default on a TTY), `json`, `ndjson`, `csv`, `parquet` |
| `--out PATH` | n/a | write output to a file |
| `--units auto\|indian\|intl\|raw` | n/a | display units, see 6.4 |
| `--asof DATE` | n/a | evaluate as of a date; never the wall clock |
| `--journal` | n/a | log request, response hash and `asof` to `data/journals` |
| `--fetch auto\|never\|force` | n/a | missing-data policy, default `auto` (section 12.5) |
| `--source NAME` | n/a | restrict gap fill to one provider |
| `--on-missing error\|skip\|warn` | n/a | what to do for instruments whose gap could not be filled |
| `--max-fetch N` | n/a | confirm before fetching for more than N instruments |

`json` output is the `ScreenerScanResponse` unchanged, with raw numbers.

### 3.3 Exit codes

| Code | Meaning |
|---|---|
| 0 | Success (zero rows is still success) |
| 1 | Invalid input: parse error, unknown metric, unit not valid for metric |
| 2 | Data or source error: missing catalog, unreadable data |

Errors go to stderr. A parse error shows the filter text with a caret under the offending token and a
"did you mean" list from the catalog.

## 4. Examples

```
honba screener scan --market india --timescale 1d --sort market_cap:desc --limit 20 \
  market cap between 5000 Cr and 2 Tn and rsi below 30 and close near 52 week low

honba screener scan --market india sector in IT, Banks and pe ratio under 25 \
  and volume at least 5 Lk and delivery pct at least 50

honba screener scan --timescale 1w close at 52 week low or rsi on 1d above 70

honba screener explain --market india market cap above 10000 Cr
honba screener scan --market india --print-request market cap above 10000 Cr > req.json
honba screener scan --request req.json --format json
```

## 5. Filter language

The language lives in a reusable package, `honba/query/`, so any app can accept trailing English filters.
It has no I/O and no dependency on typer.

### 5.1 Grammar

```
filters    := or_expr
or_expr    := and_expr { "or" and_expr }
and_expr   := term { "and" term }
term       := "either" or_expr "end"            -- explicit grouping
            | filter
filter     := metric [ "on" TIMEFRAME ] [ "for" PERIOD ] predicate
predicate  := cmp quantity
            | "between" quantity "and" quantity
            | ("in" | "not in") value { "," value }
            | ("contains" | "like") text
            | ("crosses above" | "crosses below") operand
            | ("at" | "near") preset_target
            | "within" quantity "of" preset_target
operand    := quantity | metric
```

- `and` binds tighter than `or`. `either … or … end` groups explicitly; parentheses work when quoted.
- The `and` inside `between X and Y` belongs to `between`; it is never read as a conjunction.
- The filler words `is` and `are` are accepted and ignored before a comparison.
- Keywords are case-insensitive. Values in `in (…)` keep their case.

### 5.2 Comparison phrases

| Phrase | `FilterOp` |
|---|---|
| `above`, `over`, `greater than`, `more than`, `>` | `GT` |
| `at least`, `no less than`, `>=` | `GTE` |
| `below`, `under`, `less than`, `<` | `LT` |
| `at most`, `no more than`, `<=` | `LTE` |
| `equals`, `is`, `=` | `EQ` |
| `is not`, `not equal to`, `!=` | `NEQ` |
| `between X and Y` | `BETWEEN` |
| `in …` / `not in …` | `IN` / `NOT_IN` |
| `like`, `contains` | `LIKE` / `HAS` |
| `crosses above`, `crosses below` | `CROSSES_ABOVE` / `CROSSES_BELOW` |

Words are preferred over symbols because `<` and `>` are redirects in bash and fish, and `&&` and `||` are command
separators. The symbol forms parse only when quoted.

### 5.3 Metric names

Metrics are matched against the catalog by **longest alias**, case-insensitively. Aliases are catalog data, not parser
code:

- `market cap`, `mcap`, `market_cap_basic` resolve to one key.
- `52 week low`, `52w low`, `52w-low` resolve to one key.
- Ambiguous matches are an error that lists the candidates.

### 5.4 Timeframe and period

- `--timescale` sets the default; `on <timeframe>` overrides it for one filter (`on 1d`, `on weekly`, `on 15 min`).
- A timeframe on a metric whose `has_timeframe` is false is an error.
- `for <period>` (`ttm`, `fy`, `last quarter`) maps to `MetricPeriod` and is checked against `has_period`.

### 5.5 Presets

A preset is a named criterion that expands into one or more predicates, defined as data:

| Phrase | Expansion |
|---|---|
| `at 52 week low` | `close <= min(low, 252 bars)` |
| `near 52 week low` | `close <= min(low, 252 bars) * (1 + tol)` (default `tol` 5%) |
| `within N% of 52 week low` | same with `tol = N%` |
| `at` / `near` / `within` `52 week high` | symmetric, using `max(high, 252 bars)` |

Fewer than 252 bars of history means insufficient data: the instrument is excluded and counted in a warning, not
treated as a match.

## 6. Quantities and units

### 6.1 Syntax

`NUMBER [SUFFIX]`, with or without a space (`10Cr`, `10 cr`, `1.5 Tn`). Suffixes are case-insensitive and have long
forms.

| Suffix | Multiplier | Long forms | Defined in |
|---|---|---|---|
| `K` | 1e3 | thousand | shared |
| `Lk`, `L` | 1e5 | lakh, lakhs, lac | `honba.india` |
| `Mn` | 1e6 | million | shared |
| `Cr` | 1e7 | crore, crores | `honba.india` |
| `Bn` | 1e9 | billion | shared |
| `Tn` | 1e12 | trillion | shared |
| `%` | 1e-2 | percent | shared |

### 6.2 Rules

- Conversion happens at parse time. `market cap between 5000 Cr and 2 Tn` becomes plain numbers (`5e10`, `2e12`) in
  `ScreenerFilterPredicate`. The wire contract stays unit-free and numeric.
- A suffix is valid only for metrics whose `unit` is money (`CURRENCY`, `PRICE`) or `SHARES`. `%` is valid for `PCT`
  and `RATIO`. Any other pairing, such as `rsi below 30 Cr`, is an exit-1 error.
- Currency markers `₹`, `Rs`, `INR` are accepted and checked against the market's currency (india is INR). Other
  currencies are rejected; FX is out of scope.
- Values are parsed as decimals and converted to `f64` once, so `1.1 Cr` is exactly `11000000.0`.

### 6.3 Market packs own the unit table

The language asks the active market pack for its suffix table (ADR 005). The India pack registers `Lk` and `Cr` in
addition to the shared set; a market without such conventions registers only the shared suffixes. Indian-market rules
therefore stay in the `india` module and the filter language stays generic.

### 6.4 Display

`--units auto` (default) formats by the market: Indian digit grouping (`₹12,34,567`) and `Cr`/`Lk` for large values
in India, `Mn`/`Bn`/`Tn` otherwise. `indian`, `intl` and `raw` force a choice. Machine formats (`json`, `ndjson`,
`parquet`) always emit raw numbers.

## 7. Architecture (DDD layering)

| Layer | Package | Contents | I/O |
|---|---|---|---|
| Domain | `honba/query/` | tokenizer, grammar, AST, quantity parser, alias resolution | none |
| Domain | `honba/screener/` | metric catalog, presets, predicate evaluation over bar series | none |
| Domain | `honba/india/` | `Lk`/`Cr` units, universes, calendar | none |
| Wire model | `honba/entities/screener.py` | `ScreenerScanRequest`, `ScreenerFilterPredicate`, `FilterOp`, `Timeframe`, … | none |
| Port | `honba/screener/ports.py` | `ScreenerSource.scan(request) -> ScreenerScanResponse` | defined here |
| Infrastructure | `honba/screener/sources/` | local Parquet catalog source; later an HTTP source for `POST /api/v1/screener/scan` | yes |
| Edge | `honba/cli/screener.py` | typer layer: parse options, call parser and port, render | yes |
| Edge | `honba/ai/mcp` | `screener_scan`, `screener_metrics`, `screener_presets` taking the wire model | yes |

The CLI does only: arguments → `ScreenerScanRequest` → port → render. The same request goes through the MCP tools,
so humans and agents share one contract. Evaluation reuses the indicator bank in `honba.strategies.indicators`
instead of reimplementing indicators.

The metric catalog is seeded from `metric_catalog.json` (about 55 metrics with TradingView-style wire keys such as
`market_cap_basic`, `price_52_week_low` and `RSI`, plus India extras), not invented here. Natural-language aliases,
per-metric lookback and the `uiId` mapping are layered on top of that seed; see section 14.2.

## 8. Determinism and the research loop

- `--asof` is required for historical scans and defaults to the latest bar date in the catalog, never the wall clock.
- `--print-request` makes any command a replayable artifact; `--journal` records request, response hash and `asof`
  in `data/journals` for the LLM research loop.
- Output order is stable: ties on the sort key break by `fullSymbol`.

## 9. Testing plan

Written first (red), per the workspace TDD rule.

Unit (`python/tests/unit/`, no I/O):
- Quantities: every suffix, case, spacing, decimals, invalid pairing with a non-money metric, currency markers.
- Grammar: every comparison phrase, `between … and` versus conjunction, `and`/`or` precedence, `either … end`,
  alias longest-match and ambiguity, timeframe and period overrides, error carets and suggestions.
- Presets: 52-week boundaries at exactly 252 bars, fewer than 252, and ties.
- Catalog: `filterable`, `has_timeframe`, `has_period` enforcement.
- Evaluator: hand-built bar series per operator, including crossover edge cases.

Integration (`python/tests/integration/`, fixture Parquet catalog, no network):
- `CliRunner`: the English form, `--request`, and `--print-request` output agree.
- Source contract test, run against every `ScreenerSource` implementation.
- Golden vector in `schema/golden` for a CLI-built request, run through the Rust round-trip.
- Shell-safety: the documented example commands parse from separate words and from one quoted string.

Rust: if the evaluator is ever moved to Rust, unit tests go in `crates/<crate>/src/tests/` and integration tests in
`crates/<crate>/tests/`, and the Python and Rust results are compared on shared golden vectors.

## 10. Rollout

- [x] **0. Prerequisites from section 14.6**: Rust and Python screener wire parity, catalog seed placement, and the `fullSymbol` format. (Completed in commits `8848b1b`, `2c35c27`, `039f9c1`, `74f7c5a`).
- [x] **1. `honba/query/` quantity parser and grammar, with tests**: (Completed in commit `0dd9ce4` - `honba.query.quantity`, `honba.query.parser`, and unit tests).
- [x] **2. Metric catalog (with per-metric lookback) and presets, with tests**: (Completed in commits `039f9c1` and `0dd9ce4` - `honba.screener.catalog`, `honba.screener.presets`, unit tests).
- [x] **3. Coverage ledger, interval algebra and gap planner (pure), then `BarStore`/`MarketDataProvider` ports with the in-memory and local implementations and the contract tests (section 12)**: (Completed - `honba.screener.coverage`, `honba.screener.ports`, `honba.india.calendar`, contract tests).
- [x] **4. Evaluator and the `ScreenerSource`, wired through `DataService.ensure`**: (Completed - `honba.screener.evaluator`, `honba.screener.sources`, `DataService`).
- [x] **5. `honba screener` commands (`scan`, `explain`, `metrics`, `presets`) and `honba data coverage|gaps|fetch`**: (Completed - CLI scanning with formatting, explain, catalog browsing, data coverage, gaps, and fetch).
- [x] **6. Knowledge pack generation and drift check, `LlmPort`, the `ask` pipeline with a scripted fake LLM (section 13)**: (Completed - `honba.ai.knowledge`, `honba.ai.llm.provider`, `honba.ai.ask`, `honba ai knowledge export|check`, and `honba screener ask`).
- [ ] **7. Local-model and remote `LlmPort` implementations, eval harness, `save`/`run`, journal integration, MCP tools**.
- [ ] **8. Follow-ups in sibling repos, flagged but not done here**: `honba-docs` (CLI reference and filter language), `honba-frontend` (shares the request model and the same `scan` endpoint).

## 11. Open points

- Data source: superseded by section 12. The store is append-only and coverage-aware, and missing ranges are fetched
  from providers on demand. Tests never use the network (fake provider).
- Whether the full expression form (`close <= 52 week low * 1.05`) should follow the structured English form. It is
  deferred; the grammar leaves room for it in `operand`.
- The wire models in `honba/entities/screener.py` and `honba-entities/src/screener.rs` are currently uncommitted work
  in this repo. This design depends on them and should land after, or together with, that work.
- "In-process LLM" is read here as a model running inside the same process (a local model in Python, WebGPU in the
  browser), with remote APIs as an opt-in alternative behind the same port. Confirm that reading and pick the local
  runtime.
- Corporate-action handling (raw bars plus adjustment factors) needs its own ADR before step 3.
- The restricted-Python tier (13.2) ships after tier 1 is accurate on the eval set, behind an opt-in flag.

## 12. Data coverage and gap fill

### 12.1 Problem

The store holds RELIANCE 1d bars for 2022-01-01 to 2025-01-01. A request that reaches past either end, or into an
interior hole, must fetch only the missing range from known sources, append it, and then run the action. A request
fully inside the covered range must make no external call.

### 12.2 Storage

Behind a `BarStore` port, so the engine is an implementation detail. Recommended default:

- **Bars:** the Parquet catalog already in `honba-feed`, as append-only immutable segments partitioned by
  bar spec/symbol/year. Reads dedupe on `(venue, symbol, bar spec, ts_event)`.
- **Coverage ledger:** a small transactional table in the same embedded SQL store as the metric facts (section 14.3;
  DuckDB by default, which also reads the Parquet bars natively). One row per fetched range: `venue`, `symbol`, the bar
  spec (`step`, `aggregation`, `price_type`, matching `BarType`), `adjustment`, `source`, `start_ns`, `end_ns` (UnixNanos
  of session boundaries, half-open), `status`, `row_count`, `checksum`, `fetched_at_ns`. This replaces the earlier
  `instrument_id`/`timeframe`/SQLite sketch.
- `status` is `final`, `provisional` or `empty`. `empty` records a range that was fetched and has no data (before the
  listing date, suspension, holiday-only), so it is never fetched again. `provisional` marks the current session
  before the close; it is replaced, not appended, on the next request.
- **Adjustments:** bars are stored unadjusted. Splits, bonuses and dividends are a separate adjustment-factor series
  and `adjustment` is a ledger dimension. A new corporate action invalidates adjusted views only.

### 12.3 Required range and gap planning (pure domain)

1. Each catalog metric declares its lookback in bars (`52 week low` needs 252 sessions). The planner walks the filter
   AST and computes, per instrument, `required = [asof - max lookback, asof]` in **sessions** using the India
   calendar, so weekends and exchange holidays are never gaps.
2. `gaps = required - covered`, with interval arithmetic on half-open session ranges. Results are zero, one or two
   edge gaps plus any interior holes. Adjacent gaps merge, and gaps smaller than a configurable number of sessions
   merge into a neighbouring one to save requests.
3. Example: covered `[2022-01-01, 2025-01-01)`, requested `[2021-06-01, 2025-06-30]` gives gaps
   `[2021-06-01, 2022-01-01)` and `[2025-01-01, 2025-06-30]`. Only these two are fetched.

### 12.4 Ports and flow

- `MarketDataProvider.fetch(instrument, timeframe, range) -> list[Bar]`. Implementations:
  - **Option A (Implemented):** Public daily EOD archives (`NseBhavcopyProvider` in `honba.research.data_loader.nse`) supporting modern UDiFF CM Bhavcopy zip and Sec Bhavdata full CSVs with local directory caching.
  - **Option B (Scheduled / Recorded):** Broker APIs (Zerodha Kite, Upstox, Dhan via `honba-adapters`) for authenticated historical intraday candles (`15m`, `5m`, `1m`) and live market feed tick streaming.
  - Providers are an ordered list from config with fallback. Each provider declares its limits (for example intraday history depth), so an impossible request fails fast with a clear message.
- `BarStore`: `coverage()`, `read()`, `append()`.
- Application service `DataService.ensure(plan)`:
  1. fetch each gap with bounded concurrency, retry with backoff and rate limiting;
  2. validate every bar against the `Bar` invariants (finite, `low <= open, close <= high`, `volume >= 0`) and reject
     bad rows;
  3. write the segment to a temporary file, rename it, and update the ledger in one transaction;
  4. return a read view over the now-complete range.
- Re-running is idempotent. A partial failure keeps the gaps that succeeded and reports the ones that failed;
  `--on-missing` decides whether the action proceeds.
- The interval algebra and planner are domain code with no I/O. Providers and the store are infrastructure at the
  edge. The CLI only calls `DataService`.

### 12.5 CLI behaviour

- `--fetch auto` (default) fills gaps; `never` is offline mode and reports the shortfall (exit 2, or a warning with
  `--on-missing skip`); `force` refetches the requested range.
- `--max-fetch N` prompts before a universe scan triggers fetches for more than N instruments.
- `explain` prints the fetch plan, for example `RELIANCE 1d: fetch 2021-06-01..2021-12-31 and 2025-01-02..2025-06-30
  from nse`.
- `honba data coverage [SYMBOL]` shows covered ranges, `gaps` shows what a request would fetch, and `fetch` fills a
  range without running a scan.

### 12.6 Determinism

Fetched data is stored, so a rerun is offline-reproducible. The journal records a hash of the ledger rows the run
depended on. Tests use a fake provider, an in-memory store and a simulated clock; none touch the network.

## 13. Natural language to query (in-process LLM)

### 13.1 Principle

The LLM is a translator from free text into representations that already have validators. It never executes anything
and never replaces the parser. Treat its output as untrusted input (workspace rule for AI-generated artifacts).

### 13.2 Output tiers

| Tier | Output | Validated by | Status |
|---|---|---|---|
| 1 (default) | an English filter sentence in the section 5 grammar, or a `ScreenerScanRequest` JSON | the parser and the wire model | v1 |
| 2 (opt-in) | a restricted Python predicate for conditions the grammar cannot express | an AST allow-list, then the same evaluator | after tier 1 is accurate |
| 3 | a full strategy | the existing `ai/verification` and strategy tests | out of scope here |

Tier 2 allows only: column names (`open high low close volume`), whitelisted indicator-bank functions, arithmetic,
comparisons, boolean operators and numeric literals. It rejects imports, attribute access beyond the whitelist,
dunder names, comprehensions with unbounded size, and any call outside the whitelist. It has size and time limits and
no network, file or process access.

### 13.3 Pipeline for `honba screener ask`

```
free text -> select knowledge slice -> LLM (constrained) -> parse & validate
          -> on error: feed the caret message back (at most 2 repairs)
          -> show translation: English echo, resolved request, fetch plan
          -> confirm (--yes for scripts) -> DataService.ensure -> scan
```

Nothing runs on unvalidated output. The translation is also printed as the equivalent
`honba screener scan ...` command, so users learn the grammar and can replay it without the LLM.

### 13.4 LLM port and runtimes

`LlmPort.complete(messages, grammar | schema, seed, temperature=0) -> str`.

| Implementation | Where | Notes |
|---|---|---|
| Local in-process (Python) | the CLI/backend process | for example llama-cpp-python or transformers; grammar-constrained decoding makes syntactically invalid output impossible; default, so queries stay on the machine |
| Remote API | the CLI/backend | existing `ai` extra (litellm, anthropic, openai); opt-in |
| Browser in-process | the frontend | WebLLM or transformers.js on WebGPU, using the same knowledge pack and grammar |
| Scripted fake | tests | deterministic responses |

Model choice is config (`configs/ai/*.toml`). No model is bundled.

### 13.5 Knowledge pack: how the LLM knows honba internals

No fine-tuning and no reading source at runtime. A **generated, versioned, content-hashed artifact** built from the
single sources of truth:

- the grammar (EBNF, converted to a GBNF grammar and a JSON schema for constrained decoding);
- enums from the wire contract (`FilterOp`, `Timeframe`, `MetricPeriod`);
- the metric catalog: key, aliases, unit, value type, timeframe and period flags, lookback bars;
- presets, the unit table of each market pack, universe names, indicator specs from `indicator_spec`;
- the JSON Schema of the request and response (`schema/domain`);
- golden examples: free text, the English filter, and the resolved request. These are the same fixtures the tests and
  the eval use, so documentation cannot rot;
- short internals notes: the data flow (query, request, `DataService`, evaluator) and what is and is not supported, so
  the model can answer "cannot express that" instead of guessing.

Delivery: small local models have small context windows, so a retrieval step picks the relevant catalog entries and
the nearest few-shot examples for the query, always including the grammar and enums. Larger models can take more of
the pack. The same artifact is exposed three ways: a Python module `honba.ai.knowledge`, MCP resources in the gateway
for external agents, and generated TypeScript for the frontend (same workflow as `make schema`). `honba ai knowledge
check` fails CI on drift, like `make check-schema`.

### 13.6 Safety

- The parser, unit rules and catalog checks gate everything. Tier 2 code runs only inside the allow-list sandbox.
- User text and instrument names enter prompts as quoted data. Data-source strings (company names) can contain
  instructions and are never treated as instructions.
- `ask` is read-only: it can screen and fetch data, never place orders.
- The journal records the knowledge-pack hash, model id, seed, raw output and the validated request.

### 13.7 Frontend

The frontend uses the same pack and grammar. For v1 it validates by calling the backend
(`POST /api/v1/screener/explain`), so there is one parser and no drift. A generated TypeScript parser for offline use
is a later option, checked against the same golden vectors. Frontend changes are a follow-up in `honba-frontend`,
flagged but not made here.

### 13.8 Tests

- Unit: knowledge-pack generation is deterministic and contains every catalog key, alias and enum value (a drift
  test); retrieval picks the expected slice; the repair loop with a scripted fake LLM (fail twice then pass, and fail
  past the limit gives exit 1); the tier 2 validator rejects imports, dunder access, attribute escapes and
  non-whitelisted calls.
- Integration: `ask` end to end with the fake LLM through parser, `DataService` with the fake provider and the gap
  fill, and the scan; the `LlmPort` contract test across implementations; the eval harness over recorded responses.
- Real-model runs are marked `slow` and run in a separate CI step. Model accuracy (exact match and
  execution-equivalence rates on the golden set) is reported by `honba ai eval screener`, not asserted in unit tests.

### 13.9 External-agent integration (skill pack)

Reference: HydraTrade's `agent/` folder (`HydraLabs-RF/HydraTrade`). It takes the opposite route from 13.4: the LLM is an
external coding assistant (Cursor, Claude Code) that drives a thin CLI, and the app's knowledge is a markdown skill file.
This was read from the repo's README and plugin spec; the code itself was not inspected. We adopt its good parts
alongside the in-process path, so one knowledge pack serves both:

- **Skill files generated from the pack.** `honba ai skills install --target claude|cursor|both` writes
  `.claude/skills/honba-screener/SKILL.md` and the Cursor equivalent. The content comes from the knowledge pack
  (grammar, catalog, units, examples), not hand-written prose, and the drift check covers it.
- **The CLI is the tool surface.** `--format json`, `--print-request`, `explain` and the exit codes in 3.3 are what let
  an external agent drive Honba reliably. Commands that produce reports return their file paths.
- **Thin commands.** No screening or trading logic in the skill or CLI layer; they call the library, as in section 7.
- **Safety split.** The screener and data commands are read-only and safe for model invocation. Any future live or
  order command is excluded from model invocation (`disable-model-invocation`) and requires an explicit `--yes`.
- **Public and private layers.** The public skill ships the grammar and catalog. A git-ignored private overlay
  (`~/.config/honba/skills/`) holds the user's own universes, naming conventions and risk notes.

What we add beyond a prose skill: generated content, parser validation of whatever the model produces, and
grammar-constrained decoding on the local path.


### 13.10 MCP gateway (reference: Jesse's `jesse/mcp`)

Reference: `jesse-ai/jesse`, `jesse/mcp/`. Read from `agent_rules.md`, `USAGE_LIMITS.md`, `server.py` and the folder
listing through a summarizing fetch, not the full code. Jesse runs a FastMCP server over streamable HTTP, registers
`tools/` (one module each for backtest, candles, indicator, strategy, Monte Carlo, optimization, significance test,
config) and `resources/` (`jesse://strategy` and others), and ships a rules file for the agent. We already plan an MCP
gateway at `honba/ai/mcp`; these are the patterns we take from it.

1. **Fire and poll for long operations.** `run_backtest` returns immediately and `get_backtest_session` is polled to a
   terminal status; candle import returns an `import_id` that can be resumed. Honba uses the same shape for anything
   that can take long (a universe scan with gap fill, a bulk fetch):
   - `screener_scan_start(text | request)` returns `{scan_id}`;
   - `screener_scan_get(scan_id)` returns `status` (`queued`, `fetching`, `running`, `done`, `failed`), progress, the
     fetch plan, and the `ScreenerScanResponse` when done;
   - `data_fetch_start` and `data_fetch_get` work the same way and are resumable by id.
   Small scans accept `wait=true` with a timeout and return the result directly.
2. **Gap fill stays in the app, not in the agent.** Jesse tells the agent not to pre-check candles, to let the backtest
   fail on missing data, then import from about two months before the start date and retry. That puts a retry loop and a
   guessed warmup in the agent. Honba's `DataService` (section 12) fills gaps deterministically with the exact lookback
   per metric, and the agent only sees the structured fetch plan and result. `--fetch never` remains for agents that
   want control.
3. **Discovery before generation.** Jesse requires `list_indicators` then `get_indicator_details` before writing
   indicator code. Honba exposes `screener_metrics_list`, `screener_metric_get` and `screener_presets_list`, plus the
   knowledge pack as resources (`honba://knowledge/grammar`, `/metrics`, `/units`, `/examples`) so agents look up
   metric keys and units instead of guessing.
4. **Agent rules shipped with the server.** A versioned `honba://rules` resource, also placed in the server
   instructions and in the skill file from 13.9. Rules adopted from Jesse: use the tools rather than working around
   them; surface tool errors instead of patching around them; never invent results, and say what data is missing; if
   the tool server is unavailable, stop and tell the user; always give the user the result id or link.
5. **Structured results, not exceptions.** Errors and refusals return typed payloads (code, message, parse position,
   suggestions, `budget_exhausted` with a reset time) so an agent can act on them. This matches the exit codes and
   caret messages in 3.3.
6. **Budgets for expensive operations.** Jesse meters four heavy tools with daily credits. Honba is local and open
   source, so the goal is to protect against runaway agents, not to bill: configurable per-session limits on
   instruments fetched, provider requests and LLM calls (`--max-fetch` and its MCP equivalent), reported as
   `budget_exhausted` rather than an error.
7. **Thin tools over services.** One tool module per area (`screener`, `data`, `knowledge`) calling the same services
   as the CLI. No logic in the tool layer, mirroring Jesse's `tools/` over `tools/services/`.
8. **Results link back to the UI.** A completed scan has a `scan_id` the frontend can open, as Jesse includes
   dashboard URLs in replies. The journal entry (section 8) is keyed by the same id.
9. **Transport and auth.** Stdio for local use, streamable HTTP for the frontend and remote agents, with a token passed
   at startup. Nothing binds beyond localhost by default (Jesse's server listens on `0.0.0.0`; we do not copy that).

## 14. Alignment with the gaps pack (`honba-labs/tmp/gaps/`)

Inputs read: `Readme.md`, `mapping.md`, `rest.md`, `persistance.sql`, `metric_catalog.json`,
`rust_screener_gap.rs`. That pack fixes the data contract this CLI sits on. Where this document disagreed with it, the
pack wins and the sections above were edited. Nothing in `tmp/gaps/` was modified.

### 14.1 Contract decisions adopted

- **Wire format:** JSON wire, pydantic `_Wire` and serde (ADR 006). No protobuf. Screener models use camelCase
  aliases on the wire (`primaryOnly`, `columnSet`, `fullSymbol`); engine goldens stay snake_case. The CLI and the
  filter language print and accept wire names only.
- **Identity:** `InstrumentId {symbol, venue}`, displayed `RELIANCE.NSE`. `ScreenerRow.fullSymbol` becomes
  `RELIANCE.NSE`; `NSE:RELIANCE` is accepted on input as an alias. Filter text and `--universe` accept both.
- **Numbers and time:** money is `f64` plus the `Currency` enum, never decimal strings; timestamps are UnixNanos, no
  RFC3339 on new fields. The unit parser (section 6) already converts to `f64`. `--asof DATE` is converted at the edge
  to the UnixNanos of the IST session close, and row-level `asOf`, if added, is nanos.
- **Storage:** lean engine `Instrument`; research facts live in `honba-data`, not in the engine crates.
  Parquet first, ClickHouse optional later. Brokers and credentials stay in `honba-adapters`; no broker tables here.
- **Asset types:** the scan `types[]` enum is `EQUITY, FUTURE, OPTION, FX, INDEX, MUTUAL_FUND, ETF, BOND, IPO`, which
  needs `Etf`, `Bond`, `Ipo` added to `InstrumentKind` in Rust and Python. The UI's plural names (`stocks`, `mf`,
  `bonds`) exist at the UI boundary only; the CLI accepts them as aliases.

### 14.2 Metric catalog: seed, aliases, and the gaps in it

`metric_catalog.json` is the seed. Wire key = `MetricKeySpec.key`; `uiId` = the frontend `columns.tsx` id. UI ids never
go on the wire. The natural-language layer adds, as data beside the seed:

- `aliases` (phrases such as `market cap`, `52 week low`, `p/e`) and `lookbackBars` (for gap planning, section 12.3).
- A build-time check that aliases are unique across the whole catalog. This matters because `uiId` and wire key
  collide in places: UI `change` is the absolute change (`change_abs`), while wire `change` is the percentage. A bare
  `change` in filter text is therefore ambiguous and reports both candidates; `change %` and `change abs` resolve.
  Wire keys are case-sensitive (`RSI`, `SMA200`, `Perf.1M`); aliases match case-insensitively.

Problems found in the seed that block parts of this design:

1. **No SECURITY metrics.** `mapping.md` lists `symbol`, `name`, `exchange`, `country`, `sector` as SECURITY metrics and
   the catalog declares the group, but `metric_catalog.json` has no entries for them. Filters like
   `sector in IT, Banks` and `exchange in NSE, BSE` need them. Add them.
2. **Fixed-period indicators only.** The catalog has `SMA20` and `SMA200` but no `SMA50` or `RSI` periods. The example
   `50 day sma crosses above 200 day sma` needs `SMA50`. Either add the common periods as catalog entries (the
   TradingView approach) or add a parametric metric family (`SMA{n}`); this document assumes the first, and the
   examples will use `SMA20` and `SMA200` until `SMA50` exists.
3. **Metric-to-metric operands are not on the wire.** `ScreenerFilterPredicate.value` is `Any`, so `crosses above SMA200`
   has no defined encoding. The wire needs an agreed shape, for example `{"key": "SMA200"}` as a metric reference,
   with a golden vector in both languages. Until then the CLI and `ask` support constants only for the comparison
   operators, and crossovers are listed as blocked.
4. **`uiId` is not in the Python `MetricDefinition`.** The model is `extra="forbid"`-style (`_Wire`), and the JSON and
   `persistance.sql` both carry `ui_id`. The model gets an optional `uiId` field, or the loader strips it.
5. **Type drift:** the Rust sketch types `default_period` and `default_timeframe` as enums and the Python model uses
   strings. They must match before the schema export is trusted.
6. Hand-checked, not machine-validated: no test currently loads the JSON against `MetricDefinition`. A catalog-load
   test is the first catalog test (section 9).

### 14.3 Two evaluation modes, one request

The pack models screener facts as a table, `instrument_metrics (venue, symbol, metric_key, period, timeframe,
value_num, value_text, as_of_ns)`, keyed so each metric has one latest value. That differs from my earlier
assumption that every filter is computed from bars. Both are real, so `ScreenerSource` has two strategies behind the
same request:

| Mode | Used when | Data | Gap handling |
|---|---|---|---|
| Facts scan | latest values (`--asof` omitted) | `instrument_metrics` joined to `instruments` | refresh metrics whose `as_of_ns` is older than a per-source freshness policy |
| Bar-derived | historical `--asof`, or a metric with no stored fact | Parquet bars through the evaluator | section 12 gap fill of bars |

- `source` on each metric decides how it is refreshed: `MARKET` and `CALCULATED` can be recomputed from bars;
  `FUNDAMENTAL` and `ANALYST` come from external providers and only have freshness, not bar coverage.
- After bars are gap-filled, the `CALCULATED` and `MARKET` facts for those instruments (52-week high and low, SMAs,
  RSI, performance) are recomputed and upserted with a new `as_of_ns`, so the two modes never disagree.
- Historical scans cannot use the facts table, because it keeps only the latest row per key. `--asof` therefore forces
  bar-derived mode and fails clearly for `FUNDAMENTAL` metrics, which have no history stored.
- `52 week low` has two meanings. In facts mode `close at 52 week low` compares stored `close` and `price_52_week_low`;
  in bar-derived mode it is computed from 252 sessions. The preset expands to the same predicate and the mode is an
  implementation detail, with a test asserting both modes agree on a fixture.
- Persistence follows `persistance.sql`: `instruments`, `instrument_metrics` and `metric_definitions` in `honba-data`.
  The SQL is dialect-neutral, so it runs on DuckDB (default embedded engine, also holding the coverage ledger) and on
  Postgres. Bars stay Parquet.
- Timeframe handling: CLI `--timescale` is the screener `Timeframe`; stored bars are keyed by `BarSpecification` (step,
  aggregation, price type). A pure mapping function converts between them, and the REST chart timeframes (`1D`, `1W`,
  `1M`) map the same way. They are not interchangeable with the screener enum.

### 14.4 REST surface the CLI, MCP and frontend share

From `rest.md`, kept as is:

- Canonical: `GET /api/v1/metrics`, `GET /api/v1/column-sets`, `POST /api/v1/screener/scan`, plus the symbol-page
  routes `GET /api/v1/instruments/{venue}/{symbol}[/metrics?group=&period=|/financials|/delivery|/shareholding|/peers|
  /technicals?timeframe=]` and asset-type extras (`holdings`, `constituents`, `yields`, `ipo`).
- Compatibility during migration: `GET /api/instruments?country=IN&assetType=stocks&limit=4000`,
  `GET /api/instruments/{symbol}`, `GET /api/instruments/{symbol}/candles?timeframe=1D`. The wide camelCase
  `Instrument` is a projection of `instrument_metrics` through the `uiId` map; no second wide model is invented.
- HTTP lives in the Python control plane or a `honba-cli` gateway; scan execution lives in `honba-data` or the control
  plane, never `honba-engine`; live quotes come through `honba-market` and adapters.

Additions this design needs, in the same `/api/v1` namespace:

- `POST /api/v1/screener/explain`: parse and validate filter text or a request, return the resolved request, the fetch
  plan and warnings. This is the endpoint the frontend uses for validation in 13.7.
- `GET /api/v1/metrics` is also the frontend's source for the knowledge pack's metric slice, so the browser LLM and the
  backend see the same aliases and units.

The CLI can later grow an `instrument` app (`honba instrument show RELIANCE.NSE --group VALUATION`) over the symbol-page
routes. It is not part of the first cut.

### 14.5 Things not to do (carried over from the pack)

- Do not add protobuf beside ADR 006, expand the engine `Instrument` with `pe`, `roe` or `aum`, put broker credentials
  in this schema, or use RFC3339 on new research endpoints.
- Do not let the LLM or the filter language emit UI column ids. The knowledge pack lists wire keys, with `uiId` only as
  an alias source subject to the uniqueness check in 14.2.

### 14.6 Prerequisites and ownership

These are in the screener-model work, not in this design, and several touch files that are currently uncommitted in this
repo, so they are listed rather than done:

1. **Rust parity:** `MetricDefinition` and `ScreenerScanRequest` in `honba-entities` (the pack's sketch), exported from
   `lib.rs`, and both included in the `wire.py` schema export list (drift risk). Then `make schema`. Note the sketch
   names `SortSpec` while Python names `ScreenerSortSpec`; the names must match.
2. **`fullSymbol` format** changes to `RELIANCE.NSE`, with golden vectors updated in both languages.
3. **Catalog placement:** one `metric_catalog.json` as source of truth where both Python and `honba-data` can read it
   (for example under `schema/`, beside the goldens), loaded and validated in both languages by the same test.
4. **`InstrumentKind`:** add `Etf`, `Bond`, `Ipo`.
5. **Crossover operand encoding** (14.2, item 3) and the SECURITY and `SMA50` catalog entries (14.2, items 1 and 2).
6. Frontend follow-ups, flagged only: `columns.tsx` reading `uiId` from the catalog, the wide `Instrument` projection,
   and the new generated types. No other repo is touched by this design.
