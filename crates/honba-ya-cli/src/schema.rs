use anyhow::Result;
use std::path::PathBuf;

// NOTE (Phase 1 M0): the honba-tamashii-codegen contract was canonicalized to
// honba-core-only artifacts (domain JSON schema + Python stubs). The REST-side
// codegen artifacts (TypeScript client, OpenAPI 3.1, MCP tools, markdown
// reference) are re-introduced in Phase 7 (`honba schema` hardening) once the
// canonical route registry lands in honba-ya-api's wire layer.

pub(crate) fn export(
    output: &Option<PathBuf>,
    _typescript_dir: &Option<PathBuf>,
    _client_dir: &Option<PathBuf>,
    _openapi_output: &Option<PathBuf>,
    pyi_output: &Option<PathBuf>,
    _mcp_output: &Option<PathBuf>,
    _docs_output: &Option<PathBuf>,
) -> Result<()> {
    let codegen = honba_codegen::Codegen::new();
    let schema_dir = output
        .clone()
        .unwrap_or_else(|| PathBuf::from("schema/domain"));
    let p = codegen.write_json_schema(&schema_dir)?;
    println!("domain_schema.json written to {}", p.display());

    if let Some(dir) = pyi_output.as_ref() {
        let p = codegen.write_pyi(dir)?;
        println!("__init__.pyi written to {}", p.display());
    }
    if _typescript_dir.is_none() && pyi_output.is_none() {
        println!(
            "Note: pass --pyi (or a --typescript dir in Phase 7) to generate further artifacts."
        );
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
pub(crate) fn export_all(
    schema_dir: &PathBuf,
    _typescript_dir: &Option<PathBuf>,
    _client_dir: &Option<PathBuf>,
    _openapi_dir: &Option<PathBuf>,
    pyi_dir: &Option<PathBuf>,
    _mcp_dir: &Option<PathBuf>,
) -> Result<()> {
    let codegen = honba_codegen::Codegen::new();
    codegen.write_json_schema(schema_dir)?;
    if let Some(dir) = pyi_dir.as_ref() {
        codegen.write_pyi(dir)?;
    }
    println!("Domain codegen artifacts written.");
    Ok(())
}
