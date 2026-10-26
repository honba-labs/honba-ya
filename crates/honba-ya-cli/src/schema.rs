use anyhow::Result;
use honba_api_rest::openapi_json;
use std::path::PathBuf;

// NOTE (Phase 1 M0): the honba-tamashii-codegen contract was canonicalized to
// honba-core-only artifacts (domain JSON schema + Python stubs). The REST-side
// codegen artifacts (TypeScript client, OpenAPI 3.1, MCP tools, markdown
// reference) are re-introduced in Phase 7 (`honba schema` hardening) once the
// canonical route registry lands in honba-ya-api's wire layer.
// Phase 6.3: OpenAPI 3.1 is now emitted into `--openapi` from the runtime
// generator in `honba_api_rest::openapi`.

pub(crate) fn export(
    output: &Option<PathBuf>,
    _typescript_dir: &Option<PathBuf>,
    _client_dir: &Option<PathBuf>,
    openapi_output: &Option<PathBuf>,
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

    // Phase 6.3: emit OpenAPI 3.1 document into --openapi dir
    if let Some(dir) = openapi_output.as_ref() {
        let doc = openapi_json();
        let p = dir.join("openapi.json");
        std::fs::create_dir_all(dir)?;
        let pretty = serde_json::to_string_pretty(&doc).unwrap_or_default();
        std::fs::write(&p, pretty + "\n")?;
        println!("OpenAPI 3.1 document written to {}", p.display());
    }

    if let Some(dir) = pyi_output.as_ref() {
        let p = codegen.write_pyi(dir)?;
        println!("__init__.pyi written to {}", p.display());
    }
    if _typescript_dir.is_none() && pyi_output.is_none() {
        println!(
            "Note: pass --pyi (or a --openapi dir in Phase 6 / --typescript dir in Phase 7) to generate further artifacts."
        );
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
pub(crate) fn export_all(
    schema_dir: &PathBuf,
    _typescript_dir: &Option<PathBuf>,
    _client_dir: &Option<PathBuf>,
    openapi_dir: &Option<PathBuf>,
    pyi_dir: &Option<PathBuf>,
    _mcp_dir: &Option<PathBuf>,
) -> Result<()> {
    let codegen = honba_codegen::Codegen::new();
    codegen.write_json_schema(schema_dir)?;
    // Phase 6.3: emit OpenAPI 3.1 document into --openapi dir
    if let Some(dir) = openapi_dir.as_ref() {
        let doc = openapi_json();
        let p = dir.join("openapi.json");
        std::fs::create_dir_all(dir)?;
        let pretty = serde_json::to_string_pretty(&doc).unwrap_or_default();
        std::fs::write(&p, pretty + "\n")?;
        println!("OpenAPI 3.1 document written to {}", p.display());
    }
    if let Some(dir) = pyi_dir.as_ref() {
        codegen.write_pyi(dir)?;
    }
    println!("Domain codegen artifacts written.");
    Ok(())
}
