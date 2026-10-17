use crate::market::{check_row_cap, MAX_BAR_ROWS};

#[test]
fn the_cap_is_one_hundred_thousand_rows() {
    assert_eq!(MAX_BAR_ROWS, 100_000);
}

#[test]
fn a_selection_at_the_cap_is_allowed_and_one_over_is_not() {
    assert!(check_row_cap(MAX_BAR_ROWS, MAX_BAR_ROWS).is_ok());
    let detail = check_row_cap(MAX_BAR_ROWS + 1, MAX_BAR_ROWS).unwrap_err();
    assert_eq!(detail.code, honba_api::ErrorCode::ValidationInvalidRequest);
    let context = detail.context.unwrap();
    assert_eq!(context["reason"], "too_many_rows");
    assert_eq!(context["limit"], MAX_BAR_ROWS);
    assert!(detail.message.contains("from"), "{}", detail.message);
}
