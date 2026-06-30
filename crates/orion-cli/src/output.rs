//! Output rendering helpers — table, JSON, YAML.

use anyhow::Result;
use clap::ValueEnum;
use serde::Serialize;
use std::io::Write;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Table,
    Json,
    Yaml,
    Wide,
}

/// Print a `Serialize` value as JSON or YAML. For `Table` / `Wide`, callers
/// should render their own table and only delegate here on `Json`/`Yaml`.
pub fn render<T: Serialize>(fmt: Format, value: &T) -> Result<()> {
    match fmt {
        Format::Json => println!("{}", serde_json::to_string_pretty(value)?),
        Format::Yaml | Format::Table | Format::Wide => {
            println!("{}", serde_yml::to_string(value)?)
        }
    }
    Ok(())
}

pub fn print_yaml<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_yml::to_string(value)?);
    Ok(())
}

pub fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Very small table-rendering helper — left-aligned, two-space gutters.
pub fn render_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for (i, h) in headers.iter().enumerate() {
        let _ = write!(out, "{:<w$}  ", h.to_uppercase(), w = widths[i]);
    }
    let _ = writeln!(out);
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                let _ = write!(out, "{:<w$}  ", cell, w = widths[i]);
            }
        }
        let _ = writeln!(out);
    }
}
