//! Runs the `honba` binary end to end: config file -> strategy -> engine ->
//! simulated fills -> analytics -> Markdown report.
//!
//! Hermetic: the backtest uses the CLI's built-in synthetic series, files go
//! under Cargo's per-target scratch directory, and nothing touches the
//! network or Python.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn honba(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_honba"))
        .args(args)
        .output()
        .expect("failed to spawn the honba binary")
}

/// A fresh scratch directory for one test.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-cli")
        .join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path, body: &str) -> String {
    let path = dir.join("backtest.toml");
    fs::write(&path, body).unwrap();
    path.to_str().unwrap().to_owned()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn backtest_prints_a_markdown_report() {
    let dir = scratch("stdout");
    let cfg = write_config(&dir, "[strategy]\nname = \"sma_crossover\"\n");
    let out = honba(&["backtest", &cfg]);
    assert!(out.status.success(), "{}", stderr(&out));
    let report = stdout(&out);
    assert!(report.starts_with("# Performance Report"), "{report}");
    assert!(report.contains("## Trades") && report.contains("## Equity"));
    assert!(report.contains("| Trades | 2 |"), "{report}");
}

#[test]
fn backtest_writes_the_report_to_the_output_file_deterministically() {
    let dir = scratch("output");
    let cfg = write_config(
        &dir,
        "[strategy]\nname = \"sma_crossover\"\nfast = 3\nslow = 8\ntrade_size = 10.0\n",
    );
    let path = |n: &str| dir.join(n).to_str().unwrap().to_owned();

    let first = honba(&["backtest", &cfg, "--output", &path("a.md")]);
    assert!(first.status.success(), "{}", stderr(&first));
    assert!(stdout(&first).is_empty());
    let second = honba(&["backtest", &cfg, "-o", &path("b.md")]);
    assert!(second.status.success(), "{}", stderr(&second));

    let a = fs::read_to_string(path("a.md")).unwrap();
    assert!(a.starts_with("# Performance Report"), "{a}");
    assert_eq!(a, fs::read_to_string(path("b.md")).unwrap());
    // Same defaults as the explicit parameters above.
    let defaults = write_config(
        &scratch("defaults"),
        "[strategy]\nname = \"sma_crossover\"\n",
    );
    assert_eq!(stdout(&honba(&["backtest", &defaults])), a);
}

#[test]
fn backtest_rejects_an_unknown_strategy() {
    let dir = scratch("unknown");
    let cfg = write_config(&dir, "[strategy]\nname = \"nope\"\n");
    let out = honba(&["backtest", &cfg]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("unknown strategy: nope"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn backtest_reports_a_missing_config_file() {
    let dir = scratch("missing");
    let cfg = dir.join("absent.toml");
    let out = honba(&["backtest", cfg.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("reading config"), "{}", stderr(&out));
}

#[test]
fn data_load_rejects_unsupported_sources() {
    let out = honba(&["data", "load", "bars.csv", "TCS"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("unsupported source extension"),
        "{}",
        stderr(&out)
    );
}

/// Writes a two-bar Parquet file in the canonical bar layout.
fn write_bars_parquet(path: &Path) {
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use honba_feed::import::parquet_source::bar_schema;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(bar_schema());
    let col = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; 2])) };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![60_000_000_000_i64, 120_000_000_000])),
        col(10.0),
        col(12.0),
        col(9.0),
        col(11.0),
        col(100.0),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn data_load_summarises_a_parquet_file() {
    let dir = scratch("data_load");
    let file = dir.join("TCS.NSE.parquet");
    write_bars_parquet(&file);
    let out = honba(&["data", "load", file.to_str().unwrap(), "TCS"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("2 bars loaded from"), "{text}");
    assert!(
        text.contains("first: ts_event=") && text.contains("close=11"),
        "{text}"
    );
}

#[test]
fn data_load_reports_the_missing_file_by_name() {
    let dir = scratch("data_load_missing");
    let file = dir.join("absent.parquet");
    let out = honba(&["data", "load", file.to_str().unwrap(), "TCS"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("absent.parquet"), "{}", stderr(&out));
}

#[test]
fn calendars_show_echoes_the_year() {
    let out = honba(&["calendars", "show", "--year", "2025"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), "Trading calendar for year 2025\n");
}

#[test]
fn missing_subcommand_is_a_usage_error() {
    let out = honba(&[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("Usage"), "{}", stderr(&out));
}

#[test]
fn schema_export_without_output_writes_relative_to_cwd_not_the_source_tree() {
    let committed =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schema/domain/domain_schema.json");
    let before = fs::read(&committed).unwrap();
    let cwd = scratch("schema_default_dir");
    let out = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args(["schema", "export"])
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(cwd.join("schema/domain/domain_schema.json").is_file());
    assert_eq!(fs::read(&committed).unwrap(), before);
}

fn load_args<'a>(file: &'a Path, symbol: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["data", "load", file.to_str().unwrap(), symbol];
    args.extend_from_slice(extra);
    args
}

#[test]
fn data_load_prints_the_qualified_instrument_defaulting_to_nse() {
    let file = scratch("data_load_nse").join("TCS.NSE.parquet");
    write_bars_parquet(&file);
    let out = honba(&load_args(&file, "TCS", &[]));
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains("(NSE:TCS)"), "{}", stdout(&out));
}

#[test]
fn data_load_honours_the_exchange_flag_and_qualified_symbols() {
    let file = scratch("data_load_bse").join("TCS.BSE.parquet");
    write_bars_parquet(&file);
    let flagged = honba(&load_args(&file, "TCS", &["--exchange", "bse"]));
    assert!(
        stdout(&flagged).contains("(BSE:TCS)"),
        "{}",
        stderr(&flagged)
    );
    let qualified = honba(&load_args(&file, "BSE:TCS", &[]));
    assert!(
        stdout(&qualified).contains("(BSE:TCS)"),
        "{}",
        stderr(&qualified)
    );
}

#[test]
fn data_load_reports_conflicting_and_unknown_exchanges() {
    let file = scratch("data_load_bad_exchange").join("TCS.NSE.parquet");
    write_bars_parquet(&file);
    let conflict = honba(&load_args(&file, "NSE:TCS", &["--exchange", "BSE"]));
    assert!(!conflict.status.success());
    assert!(
        stderr(&conflict).contains("conflicts"),
        "{}",
        stderr(&conflict)
    );
    let unknown = honba(&load_args(&file, "NYSE:TCS", &[]));
    assert!(!unknown.status.success());
    assert!(
        stderr(&unknown).contains("NSE, BSE"),
        "{}",
        stderr(&unknown)
    );
}
