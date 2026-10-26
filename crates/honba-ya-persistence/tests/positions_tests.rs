//! Position store integration tests: long-only FIFO tax-lot math with exact
//! `rust_decimal` WAC / realized PnL, oversell rollback, and per-client
//! isolation — SQLite in-memory via `Database::connect` + `migrate`.

use chrono::{DateTime, TimeZone, Utc};
use honba_core::{Exchange, InstrumentId, OrderSide};
use honba_persistence::{Database, PositionStore, PositionStoreError, migrate};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

async fn setup() -> Database {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite::memory:");
    let applied = migrate(&db).await.expect("migrate");
    assert_eq!(applied, vec![1, 2, 3, 4, 5]);
    db
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().unwrap()
}

fn reliance() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "RELIANCE")
}

async fn buy(store: &PositionStore, client: &str, qty: Decimal, price: Decimal, at: i64) {
    store
        .apply_fill(&reliance(), client, OrderSide::Buy, qty, price, ts(at))
        .await
        .expect("buy fill");
}

#[tokio::test]
async fn fifo_three_buys_then_partial_sell_exact_math() {
    let db = setup().await;
    let store = PositionStore::new(&db);
    let client = "client-a";

    // 3 buys: 10 @ 100, 10 @ 110, 5 @ 120.
    buy(&store, client, dec!(10), dec!(100), 1).await;
    buy(&store, client, dec!(10), dec!(110), 2).await;
    let after_buys = store
        .apply_fill(
            &reliance(),
            client,
            OrderSide::Buy,
            dec!(5),
            dec!(120),
            ts(3),
        )
        .await
        .expect("third buy");
    // WAC = (1000 + 1100 + 600) / 25 = 2700 / 25 = 108.
    assert_eq!(after_buys.open_quantity(), dec!(25));
    assert_eq!(after_buys.weighted_average_cost(), dec!(108));
    assert_eq!(after_buys.realized_pnl(), dec!(0));
    assert_eq!(after_buy_rows(&store, client).await, 3);

    // Partial sell: 15 @ 130 → FIFO consumes lot1 (10 @ 100) fully and
    // 5 of lot2 (10 @ 110): realized = (130-100)*10 + (130-110)*5 = 400.
    let after_sell = store
        .apply_fill(
            &reliance(),
            client,
            OrderSide::Sell,
            dec!(15),
            dec!(130),
            ts(4),
        )
        .await
        .expect("partial sell");
    assert_eq!(after_sell.open_quantity(), dec!(10));
    assert_eq!(after_sell.realized_pnl(), dec!(400));
    // Remaining lots: 5 @ 110 + 5 @ 120 → WAC = (550 + 600) / 10 = 115.
    assert_eq!(after_sell.weighted_average_cost(), dec!(115));

    // Lot ledger matches FIFO expectations exactly.
    let lots = store.get_lots(client, &reliance()).await.unwrap();
    assert_eq!(lots.len(), 2);
    assert_eq!(lots[0].entry_price(), dec!(110));
    assert_eq!(lots[0].open_quantity(), dec!(5));
    assert_eq!(lots[1].entry_price(), dec!(120));
    assert_eq!(lots[1].open_quantity(), dec!(5));
    assert_eq!(lots[0].side(), Some(OrderSide::Buy));

    // Position getters agree with the returned row.
    let position = store
        .get_position(client, &reliance())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(position.open_quantity(), dec!(10));
    assert_eq!(position.weighted_average_cost(), dec!(115));
    assert_eq!(position.realized_pnl(), dec!(400));
    assert_eq!(position.client_id, client);
    assert_eq!(position.instrument(), Some(reliance()));
}

#[tokio::test]
async fn oversell_fails_and_rolls_back_without_mutation() {
    let db = setup().await;
    let store = PositionStore::new(&db);
    let client = "client-a";

    buy(&store, client, dec!(10), dec!(100), 1).await;
    buy(&store, client, dec!(10), dec!(110), 2).await;
    buy(&store, client, dec!(5), dec!(120), 3).await;
    store
        .apply_fill(
            &reliance(),
            client,
            OrderSide::Sell,
            dec!(15),
            dec!(130),
            ts(4),
        )
        .await
        .expect("partial sell");
    let before = store
        .get_position(client, &reliance())
        .await
        .unwrap()
        .unwrap();
    let lots_before = store.get_lots(client, &reliance()).await.unwrap();

    // Open is 10 — selling 100 must fail as a long-only violation.
    let err = store
        .apply_fill(
            &reliance(),
            client,
            OrderSide::Sell,
            dec!(100),
            dec!(130),
            ts(5),
        )
        .await;
    match err {
        Err(PositionStoreError::InsufficientPosition {
            requested, open, ..
        }) => {
            assert_eq!(requested, "100");
            assert_eq!(open, "10");
        }
        other => panic!("expected InsufficientPosition, got {other:?}"),
    }

    // Transaction rolled back: no mutated rows anywhere.
    let after = store
        .get_position(client, &reliance())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(after.open_quantity(), dec!(10));
    assert_eq!(after.realized_pnl(), dec!(400));
    assert_eq!(after.weighted_average_cost(), dec!(115));
    let lots_after = store.get_lots(client, &reliance()).await.unwrap();
    assert_eq!(lots_after, lots_before);
    assert_eq!(lots_after.len(), 2);

    // Non-positive quantities are rejected before touching the database.
    let invalid = store
        .apply_fill(
            &reliance(),
            client,
            OrderSide::Buy,
            Decimal::ZERO,
            dec!(1),
            ts(6),
        )
        .await;
    assert!(matches!(
        invalid,
        Err(PositionStoreError::InvalidQuantity(_))
    ));
}

#[tokio::test]
async fn two_clients_maintain_isolated_rows_and_lots() {
    let db = setup().await;
    let store = PositionStore::new(&db);

    buy(&store, "client-a", dec!(10), dec!(100), 1).await;
    buy(&store, "client-a", dec!(10), dec!(110), 2).await;

    // Same instrument, different client: independent row with its own WAC.
    let pos_b = store
        .apply_fill(
            &reliance(),
            "client-b",
            OrderSide::Buy,
            dec!(7),
            dec!(200),
            ts(3),
        )
        .await
        .expect("client-b buy");
    assert_eq!(pos_b.client_id, "client-b");
    assert_eq!(pos_b.open_quantity(), dec!(7));
    assert_eq!(pos_b.weighted_average_cost(), dec!(200));
    assert_eq!(pos_b.realized_pnl(), dec!(0));

    // Client A untouched by client B's activity.
    let pos_a = store
        .get_position("client-a", &reliance())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pos_a.open_quantity(), dec!(20));
    assert_eq!(pos_a.weighted_average_cost(), dec!(105));
    assert_eq!(pos_a.realized_pnl(), dec!(0));

    // Lots are partitioned per client.
    let lots_a = store.get_lots("client-a", &reliance()).await.unwrap();
    let lots_b = store.get_lots("client-b", &reliance()).await.unwrap();
    assert_eq!(lots_a.len(), 2);
    assert_eq!(lots_b.len(), 1);
    assert!(lots_a.iter().all(|l| l.client_id == "client-a"));
    assert!(lots_b.iter().all(|l| l.client_id == "client-b"));

    // Client B's sell cannot touch client A's inventory, and an unknown
    // client simply reports no position.
    let sold_b = store
        .apply_fill(
            &reliance(),
            "client-b",
            OrderSide::Sell,
            dec!(7),
            dec!(210),
            ts(4),
        )
        .await
        .expect("client-b flat sell");
    assert_eq!(sold_b.open_quantity(), dec!(0));
    assert_eq!(sold_b.weighted_average_cost(), dec!(0));
    assert_eq!(sold_b.realized_pnl(), dec!(70));
    let pos_a = store
        .get_position("client-a", &reliance())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pos_a.open_quantity(), dec!(20));
    assert!(
        store
            .get_position("client-c", &reliance())
            .await
            .unwrap()
            .is_none()
    );
}

async fn after_buy_rows(store: &PositionStore, client: &str) -> usize {
    store.get_lots(client, &reliance()).await.unwrap().len()
}
