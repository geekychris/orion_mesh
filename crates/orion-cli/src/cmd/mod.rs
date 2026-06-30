pub mod apply;
pub mod bench;
pub mod delete;
pub mod describe;
pub mod diag;
pub mod dispatch;
pub mod doctor;
pub mod generate;
pub mod get;
pub mod init;
pub mod instances;
pub mod json;
pub mod logs;
pub mod queue;
pub mod run;
pub mod schedules;
pub mod stop_restart;
pub mod up;
pub mod validate;

/// Helpers shared by several subcommands.
pub(crate) mod util {
    use anyhow::{Context, Result};
    use std::io::Read;
    use std::path::Path;

    /// Read a YAML/JSON resource document from a file path or stdin (when path is "-").
    pub fn read_yaml_input(path: &Path) -> Result<String> {
        if path.as_os_str() == "-" {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("reading stdin")?;
            Ok(buf)
        } else {
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
        }
    }

    /// Canonicalise a `Kind` string by capitalising the first letter — accepts
    /// `service`, `Service`, `SERVICE` and `services` (singular wins).
    pub fn canonical_kind(s: &str) -> String {
        let s = s.trim_end_matches('s'); // permit pluralised
        let mut c = s.chars();
        match c.next() {
            Some(first) => first.to_ascii_uppercase().to_string() + c.as_str(),
            None => String::new(),
        }
    }
}
