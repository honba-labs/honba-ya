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
    extract::{rejection::QueryRejection, FromRequestParts, Path, Query, State},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Json, Response},
};
use honba_api::{
    instrument_json, parse_instrument_id, ApiResponse, BarsQuery, BarsResponse, DepthLevel,
    DepthQuery, DepthResponse, ErrorCode, ErrorDetail, InstrumentsQuery, InstrumentsResponse,
    QuotesQuery, QuotesResponse, ResponseEnvelope,
};
use honba_ports::{BarRequest, PortError};
use serde::de::DeserializeOwned;

use crate::AppState;

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

pub(crate) fn instrument_not_found(id: &honba_messages::InstrumentId) -> Response {
    failure(
        StatusCode::NOT_FOUND,
        ErrorDetail::new(
            ErrorCode::InstrumentNotFound,
            format!("instrument {id} not found"),
        ),
    )
}

/// Reads one instrument from the master; a port failure becomes `internal_error`.
pub(crate) async fn master_get(
    state: &AppState,
    id: &honba_messages::InstrumentId,
) -> Result<Option<honba_entities::Instrument>, ErrorDetail> {
    state
        .instruments
        .get_instrument(id)
        .await
        .map_err(|e| ErrorDetail::new(ErrorCode::InternalError, e.to_string()))
}

/// The close of the latest quote, via mid when a book exists; `None` when the
/// reader holds nothing (a bar store derives bid = ask = close, so this is the
/// close; a book quotes mid, else bid).
pub(crate) async fn quote_last(state: &AppState, id: &honba_messages::InstrumentId) -> Option<f64> {
    match state.quotes.read_quote(id, None).await {
        Ok(Some(quote)) => {
            let mid = (quote.bid_price() + quote.ask_price()) / 2.0;
            if mid.is_finite() {
                Some(mid)
            } else {
                Some(quote.bid_price())
            }
        }
        _ => None,
    }
}

/// Maps a port failure to a status and an envelope code.
pub(crate) fn port_failure(error: PortError) -> Response {
    let (status, code) = match &error {
        PortError::Unsupported(_) => (StatusCode::NOT_FOUND, ErrorCode::MarketDataUnavailable),
        PortError::InvalidRequest(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::ValidationInvalidRequest,
        ),
        PortError::Unavailable(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::MarketDataUnavailable,
        ),
        PortError::Timeout => (StatusCode::GATEWAY_TIMEOUT, ErrorCode::Timeout),
        PortError::Transport(_) => (StatusCode::BAD_GATEWAY, ErrorCode::TransportError),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::InternalError),
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
                .map(instrument_json)
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
        Ok(Some(instrument)) => success(instrument_json(&instrument)),
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
    let request = match BarRequest::new(id, resolved.spec, resolved.from, resolved.to) {
        Ok(request) => request,
        Err(error) => return port_failure(error),
    };
    match state.bars.read_bars(&request).await {
        Ok(bars) => match check_row_cap(bars.len(), MAX_BAR_ROWS) {
            Ok(()) => success(BarsResponse { bars }),
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
            id.symbol() == symbol && resolved.venue.as_ref().map_or(true, |v| id.exchange() == v)
        }));
        if wanted.len() == before {
            return failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(
                    ErrorCode::InstrumentNotFound,
                    format!("no instrument for symbol {symbol}"),
                ),
            );
        }
    }
    wanted.sort();
    wanted.dedup();
    let mut quotes = Vec::with_capacity(wanted.len());
    for id in wanted {
        match state.quotes.read_quote(id, resolved.as_of).await {
            Ok(Some(quote)) => quotes.push(quote),
            Ok(None) => {
                return failure(
                    StatusCode::NOT_FOUND,
                    ErrorDetail::new(
                        ErrorCode::MarketDataUnavailable,
                        format!("no quote for {id} at the requested time"),
                    ),
                )
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
            let side = |levels: Vec<honba_ports::DepthLevel>| {
                levels
                    .into_iter()
                    .map(|level| DepthLevel {
                        price: level.price,
                        qty: level.qty,
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
