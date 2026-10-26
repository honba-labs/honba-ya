# HANDOFF — Phase 6 Implementation (honba-ya)

## Phase: 6 — REST API Hardening, JWT Auth & OpenAPI (M5)

### Current state (pre-implementation)
- Phases 1-5 done: `cargo check --workspace` clean, 10-crate topology, all tests pass.
- `honba-ya-api-rest/src/auth.rs`: JWT Bearer (HMAC-SHA256 + RS256), RBAC scopes `read`/`trade`/`admin`/`research`/`authenticated` — ALREADY IMPLEMENTED, opt-in via `--tokens-file`.
- `honba-ya-api-rest/src/openapi.rs`: OpenAPI 3.1 document generation at runtime (`/schema/openapi/v1.json`, `v2.json`) — ALREADY IMPLEMENTED, but module is private (`mod openapi;`).
- `honba-ya-cli/src/schema.rs`: `honba schema --openapi` is a no-op (underscore-prefixed, unused).
- `honba-ya-api-rest/src/reports.rs`: **VIOLATES zero-floats** — extensive `f64` arithmetic, `.0` extraction from Decimal, f64 accumulators.
- `honba-ya-api-rest/src/orders.rs`: Already uses `honba-core` types (Decimal, InstrumentId, etc.) — compliant.
- `honba-ya-api-rest/src/portfolio.rs`: Already uses `honba-core` types — compliant (501s are documented gap).
- `honba-ya-cli/src/serve.rs`: AuthArgs only has `--tokens-file`/`--audit-log-file`; no JWT config flags.
- `honba-ya-api-rest/src/lib.rs`: `mod openapi;` is private.

### Phase 6 scope (from plan.md)
- **6.1** Refactor `orders.rs`, `portfolio.rs`, `reports.rs` to consume `honba-core` types; route `POST /orders`/`DELETE /orders/:id` through `RiskActor`/`honba-mon`.
- **6.2** JWT Bearer (HMAC-SHA256/RSA) + RBAC scopes `read`/`trade`/`admin`.
- **6.3** Update `honba schema` codegen to emit OpenAPI 3.1 schemas.

### Implementation plan
1. **reports.rs**: Replace all `f64` with `rust_decimal::Decimal` (zero-floats compliance). Remove `pseudo_random()->f64` and `to_decimal(f64)`.
2. **serve.rs**: Add `--jwt-secret`, `--jwt-issuer`, `--jwt-audience` flags to AuthArgs; wire into `ApiConfig.auth.jwt_config`.
3. **lib.rs (api-rest)**: Make `mod openapi` → `pub mod openapi`; add `pub fn openapi_json() -> serde_json::Value`.
4. **CLI schema.rs**: Wire `_openapi_output` to emit OpenAPI JSON via `honba_api_rest::openapi`.
5. Verify `cargo check --workspace` clean.

### Files to modify
- `honba-ya/crates/honba-ya-api-rest/src/reports.rs` (big: f64 → Decimal)
- `honba-ya/crates/honba-ya-cli/src/serve.rs` (JWT flags)
- `honba-ya/crates/honba-ya-api-rest/src/lib.rs` (pub mod openapi)
- `honba-ya/crates/honba-ya-api-rest/src/openapi.rs` (add openapi_json helper)
- `honba-ya/crates/honba-ya-cli/src/schema.rs` (wire --openapi)

### Notes
- orders.rs already routes through `honba-mon::RiskEngine::check_order` via `check_risk` — satisfies "through honba-mon". Full RiskActor routing requires the actor tree (not running in `honba serve`), out of scope.
- 6.2 JWT/RBAC already implemented; only missing CLI exposure.
- 6.3 runtime OpenAPI already implemented; only missing CLI file emission.