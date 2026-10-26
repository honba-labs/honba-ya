//! Handlers for `GET /instruments`, `GET /instruments/{id}`, `GET /bars/{id}`, `GET /quotes`
//! and `GET /depth/{id}`.
//!
//! Each handler resolves its request with the pure functions in `honba-api`,
//! calls a read port, and answers inside the standard envelope. Failures use
//! the same envelope: 404 for an unknown instrument or an unavailable
//! timeframe, 422 for a bad query, and a port failure maps by kind.

use std::sync::Arc;

use axum::{
    async_trait,
    extract::{FromRequestParts, Path, Query, State, rejection::QueryRejection},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Json, Response},
};
use honba_api::{
    ApiResponse, BarsQuery, BarsResponse, DepthLevel, DepthQuery, DepthResponse, ErrorCode,
    ErrorDetail, InstrumentsQuery, InstrumentsResponse, QuotesQuery, QuotesResponse,
    ResponseEnvelope, instrument_json, parse_instrument_id,
};
use honba_core::rust_decimal::Decimal;
use honba_feed::ports::{BarRequest, PortError};
use honba_feed::wire::UnixNanos;
use serde::de::DeserializeOwned;

use crate::AppState;

/// Converts `honba-feed`'s own instrument-kind vocabulary (really just
/// `honba_core::InstrumentKind`, reused as-is by that crate's `Instrument`) to the richer,
/// REST-contract `honba_api::wire::InstrumentKind`. Every `honba_core` variant has a direct
/// match; the wire enum's extra variants (`Etf`, `Bond`, `Ipo`, `Fx`, `MutualFund`, `Other`)
/// have no `honba_core` source today and so are never produced here.
fn to_api_kind(kind: honba_core::InstrumentKind) -> honba_api::wire::InstrumentKind {
    match kind {
        honba_core::InstrumentKind::Equity => honba_api::wire::InstrumentKind::Equity,
        honba_core::InstrumentKind::Future => honba_api::wire::InstrumentKind::Future,
        honba_core::InstrumentKind::Option => honba_api::wire::InstrumentKind::Option,
        honba_core::InstrumentKind::Index => honba_api::wire::InstrumentKind::Index,
    }
}

/// Converts a `honba-feed` reference-data record (this crate's instrument master port still
/// answers in that shape; see `state.rs`'s module doc) to the `honba_api::wire::Instrument`
/// read model `GET /instruments` actually serializes. `lot_size`/`tick_size` are `f64` on the
/// feed side (its Parquet catalog predates `Decimal`) and `Decimal` on the wire, so they are
/// converted at this one JSON-response boundary, not threaded through as `f64`.
fn to_api_instrument(instrument: &honba_feed::wire::Instrument) -> honba_api::wire::Instrument {
    honba_api::wire::Instrument::new(
        instrument.id().clone(),
        to_api_kind(instrument.kind()),
        instrument.currency(),
        Decimal::from_f64_retain(instrument.lot_size()).unwrap_or_default(),
        Decimal::from_f64_retain(instrument.tick_size()).unwrap_or_default(),
    )
}

/// Converts a `honba-feed` quote (`f64`-valued, dual `UnixNanos` timestamps) to the
/// `honba_api::wire::QuoteTick` `GET /quotes` serializes (`Decimal`-valued,
/// `chrono::DateTime<Utc>`), at the JSON-response boundary.
fn to_api_quote(quote: honba_feed::wire::QuoteTick) -> honba_api::wire::QuoteTick {
    honba_api::wire::QuoteTick::new(
        quote.instrument_id().clone(),
        Decimal::from_f64_retain(quote.bid_price()).unwrap_or_default(),
        Decimal::from_f64_retain(quote.ask_price()).unwrap_or_default(),
        Decimal::from_f64_retain(quote.bid_size()).unwrap_or_default(),
        Decimal::from_f64_retain(quote.ask_size()).unwrap_or_default(),
        quote.ts_event().to_datetime(),
        quote.ts_init().to_datetime(),
    )
}

/// Converts a `honba-feed` bar (`f64`-valued, dual `UnixNanos` timestamps) to the canonical
/// `honba_core::Bar` that `BarsResponse` serializes directly (ADR: the REST bars contract
/// reuses the domain `Bar`, not a separate wire copy), at the JSON-response boundary.
fn to_core_bar(bar: honba_feed::wire::Bar) -> honba_core::Bar {
    honba_core::Bar {
        instrument: bar.bar_type().instrument_id().clone(),
        open: honba_core::Price::new(Decimal::from_f64_retain(bar.open()).unwrap_or_default()),
        high: honba_core::Price::new(Decimal::from_f64_retain(bar.high()).unwrap_or_default()),
        low: honba_core::Price::new(Decimal::from_f64_retain(bar.low()).unwrap_or_default()),
        close: honba_core::Price::new(Decimal::from_f64_retain(bar.close()).unwrap_or_default()),
        volume: honba_core::Quantity::new(
            Decimal::from_f64_retain(bar.volume()).unwrap_or_default(),
        ),
        timestamp: bar.ts_event().to_datetime(),
    }
}

/// Query-string extractor whose rejections are the standard error envelope
/// (`validation_invalid_request`, status 422) rather than axum's plain text.
#[derive(Debug)]
pub struct ApiQuery<T>(pub T);

/// Rejection of [`ApiQuery`].
#[derive(Debug)]
pub struct ApiQueryRejection(QueryRejection);

impl IntoResponse for ApiQueryRejection {
    fn into_response(self) -> Response {
        failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorDetail::new(ErrorCode::ValidationInvalidRequest, self.0.body_text()),
        )
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiQueryRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(ApiQueryRejection)
    }
}

/// Hard cap on the bars one `GET /bars/{id}` may return.
///
/// A request selecting more is a 422 (`reason: too_many_rows`); the caller narrows `from`/`to`.
/// There is no pagination.
pub const MAX_BAR_ROWS: usize = 100_000;

/// Rejects a selection of `rows` bars larger than `limit` with a 422-bound detail.
pub(crate) fn check_row_cap(rows: usize, limit: usize) -> Result<(), ErrorDetail> {
    if rows <= limit {
        return Ok(());
    }
    Err(ErrorDetail::new(
        ErrorCode::ValidationInvalidRequest,
        format!(
            "the selection has {rows} bars, over the limit of {limit}; narrow the range with from and to"
        ),
    )
    .with_context(serde_json::json!({
        "field": "to",
        "reason": "too_many_rows",
        "limit": limit,
    })))
}

pub(crate) fn failure(status: StatusCode, detail: ErrorDetail) -> Response {
    let body: ResponseEnvelope<serde_json::Value> = ApiResponse::error(detail);
    (status, Json(body)).into_response()
}

pub(crate) fn success<T: serde::Serialize>(data: T) -> Response {
    let body: ResponseEnvelope<T> = ApiResponse::success(data);
    (StatusCode::OK, Json(body)).into_response()
}

pub(crate) fn unprocessable(detail: ErrorDetail) -> Response {
    failure(StatusCode::UNPROCESSABLE_ENTITY, detail)
}

pub(crate) fn instrument_not_found(id: &honba_core::InstrumentId) -> Response {
    failure(
        StatusCode::NOT_FOUND,
        ErrorDetail::new(ErrorCode::NotFound, format!("instrument {id} not found")),
    )
}

/// Reads one instrument from the master; a port failure becomes `internal_error`.
pub(crate) async fn master_get(
    state: &AppState,
    id: &honba_core::InstrumentId,
) -> Result<Option<honba_feed::wire::Instrument>, ErrorDetail> {
    state
        .instruments
        .get_instrument(id)
        .await
        .map_err(|e| ErrorDetail::new(ErrorCode::InternalError, e.to_string()))
}

/// Maps a port failure to a status and an envelope code.
///
/// `honba_feed::ports::PortError` has only three variants (`Unsupported`,
/// `Internal`, `Terminal`) — leaner than the old `honba_messages` taxonomy
/// this used to branch on (`InvalidRequest`/`Unavailable`/`Timeout`/
/// `Transport`), so the mapping collapses to that.
pub(crate) fn port_failure(error: PortError) -> Response {
    let (status, code) = match &error {
        PortError::Unsupported(_) => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
        PortError::Terminal(_) => (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::Conflict),
        PortError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::InternalError),
    };
    failure(status, ErrorDetail::new(code, error.to_string()))
}

/// `GET /instruments`: every instrument passing the query filters, in id order.
pub(crate) async fn get_instruments(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<InstrumentsQuery>,
) -> Response {
    match state.instruments.list_instruments().await {
        Ok(mut all) => {
            all.sort_by(|a, b| a.id().cmp(b.id()));
            let instruments = all
                .iter()
                .filter(|instrument| query.matches(instrument.id()))
                .map(|instrument| instrument_json(&to_api_instrument(instrument)))
                .collect();
            success(InstrumentsResponse { instruments })
        }
        Err(error) => port_failure(error),
    }
}

/// `GET /instruments/{id}`: one instrument, or 404 `instrument_not_found`.
pub(crate) async fn get_instrument_by_id(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
) -> Response {
    let id = match parse_instrument_id(&raw) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    match state.instruments.get_instrument(&id).await {
        Ok(Some(instrument)) => success(instrument_json(&to_api_instrument(&instrument))),
        Ok(None) => instrument_not_found(&id),
        Err(error) => port_failure(error),
    }
}

/// `GET /bars/{id}`: bars in ascending `ts_event` over `[from, to)`.
pub(crate) async fn get_bars(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    ApiQuery(query): ApiQuery<BarsQuery>,
) -> Response {
    let id = match parse_instrument_id(&raw) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let resolved = match query.resolve() {
        Ok(resolved) => resolved,
        Err(detail) => return unprocessable(detail),
    };
    match state.instruments.get_instrument(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return instrument_not_found(&id),
        Err(error) => return port_failure(error),
    }
    let request = match BarRequest::new(
        id,
        crate::catalog::to_feed_spec(resolved.spec),
        resolved.from.map(UnixNanos::from_datetime),
        resolved.to.map(UnixNanos::from_datetime),
    ) {
        Ok(request) => request,
        Err(error) => return port_failure(error),
    };
    match state.bars.read_bars(&request).await {
        Ok(bars) => match check_row_cap(bars.len(), MAX_BAR_ROWS) {
            Ok(()) => success(BarsResponse {
                bars: bars.into_iter().map(to_core_bar).collect(),
            }),
            Err(detail) => unprocessable(detail),
        },
        Err(error) => port_failure(error),
    }
}

/// `GET /quotes`: the latest (or `as_of`) quote of every instrument the symbols name.
///
/// Symbols expand across venues unless `venue` is set; results are in instrument-id order. A
/// symbol that matches no instrument is a 404 `instrument_not_found`; an instrument with no quote
/// at that time is a 404 `market_data_unavailable`, so a partial answer is never passed off as
/// complete.
pub(crate) async fn get_quotes(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<QuotesQuery>,
) -> Response {
    let resolved = match query.resolve() {
        Ok(resolved) => resolved,
        Err(detail) => return unprocessable(detail),
    };
    let mut known = match state.instruments.list_instruments().await {
        Ok(all) => all,
        Err(error) => return port_failure(error),
    };
    known.sort_by(|a, b| a.id().cmp(b.id()));
    let mut wanted = Vec::new();
    for symbol in &resolved.symbols {
        let before = wanted.len();
        wanted.extend(known.iter().map(|instrument| instrument.id()).filter(|id| {
            id.symbol == *symbol && resolved.venue.as_ref().map_or(true, |v| id.exchange == *v)
        }));
        if wanted.len() == before {
            return failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(
                    ErrorCode::NotFound,
                    format!("no instrument for symbol {symbol}"),
                ),
            );
        }
    }
    wanted.sort();
    wanted.dedup();
    let mut quotes = Vec::with_capacity(wanted.len());
    for id in wanted {
        match state
            .quotes
            .read_quote(id, resolved.as_of.map(UnixNanos::from_datetime))
            .await
        {
            Ok(Some(quote)) => quotes.push(to_api_quote(quote)),
            Ok(None) => {
                return failure(
                    StatusCode::NOT_FOUND,
                    ErrorDetail::new(
                        ErrorCode::NotFound,
                        format!("no quote for {id} at the requested time"),
                    ),
                );
            }
            Err(error) => return port_failure(error),
        }
    }
    success(QuotesResponse { quotes })
}

/// `GET /depth/{id}`: the order book, or 404 `market_data_unavailable` when the source has none.
pub(crate) async fn get_depth(
    State(state): State<Arc<AppState>>,
    Path(raw): Path<String>,
    ApiQuery(query): ApiQuery<DepthQuery>,
) -> Response {
    let id = match parse_instrument_id(&raw) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let levels = match query.levels() {
        Ok(levels) => levels,
        Err(detail) => return unprocessable(detail),
    };
    match state.instruments.get_instrument(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return instrument_not_found(&id),
        Err(error) => return port_failure(error),
    }
    match state.depth.read_depth(&id, levels).await {
        Ok(book) => {
            let side = |levels: Vec<(f64, f64)>| {
                levels
                    .into_iter()
                    .map(|(price, qty)| DepthLevel {
                        price: Decimal::from_f64_retain(price).unwrap_or_default(),
                        qty: Decimal::from_f64_retain(qty).unwrap_or_default(),
                    })
                    .collect()
            };
            success(DepthResponse {
                bids: side(book.bids),
                asks: side(book.asks),
            })
        }
        Err(error) => port_failure(error),
    }
}
