Phase 1 DONE: cargo check --workspace clean except honba-strategies (Phase 5, separately broken, untouched). plan.md updated. NEVER git reset/amend here.

[2026-10-25] Batch: honba-ya-enhancer cargo check/clippy cleanup DONE.
- Verified APIs used by `honba_strategy` from `honba-core` and fixed all compile errors in honba-ya-enhancer. `cargo check -p honba-ya-enhancer --all-targets` and `cargo clippy -p honba-ya-enhancer --all-targets -D warnings` and `cargo fmt -p honba-ya-enhancer -- --check` are clean (checked with unrelated sibling-crate edits stashed so the verify ran against exactly what was committed).
- Also required by the enhancer and included: new honba-ya-strategy/public `factory.rs` and its lib re-export; 1-line `FillReport` initializer fix in honba-ya-backtest engine.rs.
- Other pre-existing warnings remain elsewhere in the workspace `cargo clippy` gate: honba-ya-actors (5), honba-ya-persistence (4), honba-ya-api-rest (1 dead `quote_last`) — all still warnings, and are not considered in-scope for this batch.
