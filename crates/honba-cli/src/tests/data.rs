//! Unit tests for `crate::data`.

#[test]
fn load_rejects_non_parquet_sources_before_touching_the_file_system() {
    let err = crate::data::load("does/not/exist.csv", "TCS", None).unwrap_err();
    assert!(
        err.to_string().contains("unsupported source extension"),
        "{err}"
    );
}

#[test]
fn load_names_the_file_when_it_cannot_be_read() {
    let err = crate::data::load("does/not/exist.parquet", "TCS", None).unwrap_err();
    assert!(err.to_string().contains("does/not/exist.parquet"), "{err}");
}

#[test]
fn load_accepts_an_upper_case_parquet_extension() {
    let err = crate::data::load("does/not/exist.PARQUET", "TCS", None).unwrap_err();
    assert!(
        !err.to_string().contains("unsupported source extension"),
        "{err}"
    );
}

#[test]
fn load_rejects_a_conflicting_qualifier_before_touching_the_file_system() {
    let err = crate::data::load("does/not/exist.parquet", "NSE:TCS", Some("BSE")).unwrap_err();
    assert!(err.to_string().contains("conflicts"), "{err}");
}

#[test]
fn load_rejects_an_unknown_exchange() {
    let err = crate::data::load("does/not/exist.parquet", "TCS", Some("NYSE")).unwrap_err();
    assert!(err.to_string().contains("NSE, BSE"), "{err}");
}
