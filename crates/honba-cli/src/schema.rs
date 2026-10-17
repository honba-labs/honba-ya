use anyhow::Result;
use std::path::PathBuf;

pub(crate) fn export(
    output: &Option<PathBuf>,
    typescript_dir: &Option<PathBuf>,
    client_dir: &Option<PathBuf>,
    openapi_output: &Option<PathBuf>,
    pyi_output: &Option<PathBuf>,
    mcp_output: &Option<PathBuf>,
    docs_output: &Option<PathBuf>,
) -> Result<()> {
    let codegen = honba_codegen::Codegen::new();
    let schema_dir = output
        .clone()
        .unwrap_or_else(|| PathBuf::from("schema/domain"));
    let p = codegen.write_json_schema(&schema_dir)?;
    println!("domain_schema.json written to {}", p.display());

    if let Some(dir) = typescript_dir.as_ref() {
        let p = codegen.write_typescript(dir)?;
        println!("domain.ts written to {}", p.display());
    }
    if let Some(dir) = client_dir.as_ref() {
        let p = codegen.write_client(dir)?;
        println!("client.ts written to {}", p.display());
    }
    if let Some(dir) = openapi_output.as_ref() {
        let p = codegen.write_openapi(dir)?;
        println!("openapi.json written to {}", p.display());
    }
    if let Some(dir) = pyi_output.as_ref() {
        let p = codegen.write_pyi(dir)?;
        println!("__init__.pyi written to {}", p.display());
    }
    if let Some(dir) = mcp_output.as_ref() {
        let p = codegen.write_mcp(dir)?;
        println!("mcp_tools.json written to {}", p.display());
    }
    if let Some(dir) = docs_output.as_ref() {
        let p = codegen.write_docs(dir)?;
        println!("api-reference.md written to {}", p.display());
    }
    if typescript_dir.is_none()
        && client_dir.is_none()
        && openapi_output.is_none()
        && pyi_output.is_none()
        && mcp_output.is_none()
        && docs_output.is_none()
    {
        println!("Note: no --typescript/--client/--openapi/--pyi/--mcp/--docs dirs given; only domain_schema.json emitted. Pass one to generate that artifact.");
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
pub(crate) fn export_all(
    schema_dir: &PathBuf,
    typescript_dir: &Option<PathBuf>,
    client_dir: &Option<PathBuf>,
    openapi_dir: &Option<PathBuf>,
    pyi_dir: &Option<PathBuf>,
    mcp_dir: &Option<PathBuf>,
) -> Result<()> {
    let codegen = honba_codegen::Codegen::new();
    codegen.write_json_schema(schema_dir)?;
    if let Some(dir) = typescript_dir.as_ref() {
        codegen.write_typescript(dir)?;
    }
    if let Some(dir) = client_dir.as_ref() {
        codegen.write_client(dir)?;
    }
    if let Some(dir) = openapi_dir.as_ref() {
        codegen.write_openapi(dir)?;
    }
    if let Some(dir) = pyi_dir.as_ref() {
        codegen.write_pyi(dir)?;
    }
    if let Some(dir) = mcp_dir.as_ref() {
        codegen.write_mcp(dir)?;
    }
    println!("All codegen artifacts written.");
    Ok(())
}
