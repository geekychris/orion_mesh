//! `orion json` — parse free-form text on stdin into ndjson on stdout.
//!
//! Default mode is **column-header autodetect**: the first non-blank line is
//! treated as a header row, split on runs of whitespace; subsequent rows are
//! split at the same column offsets, with the trailing column absorbing any
//! extra columns so `ps -ef`'s `CMD` keeps its spaces.
//!
//! Override flags:
//!   --headers a,b,c        explicit header list (whitespace-split rows)
//!   --no-header            synthesise col_0, col_1, ...
//!   --delim csv|tsv|pipe   fixed-delimiter mode
//!   --regex '<pattern>'    per-line named-capture regex (e.g. `(?P<pid>\d+) (?P<cmd>.+)`)
//!   --passthrough          input is already ndjson, just validate

use crate::Ctx;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use regex::Regex;
use serde_json::{Map, Value};
use std::io::{BufRead, Write};

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Explicit column headers (overrides autodetect).
    #[arg(long, value_delimiter = ',')]
    headers: Vec<String>,
    /// Treat all rows as data — synthesise col_0, col_1, ... names.
    #[arg(long, conflicts_with = "headers")]
    no_header: bool,
    /// Use a fixed delimiter instead of column-header autodetect.
    #[arg(long, value_enum)]
    delim: Option<Delim>,
    /// Apply a named-capture regex to each line. Fields are the capture names.
    #[arg(long)]
    regex: Option<String>,
    /// Input is already ndjson — just validate and re-emit one object per line.
    #[arg(long)]
    passthrough: bool,
    /// Skip this many leading lines before parsing (after autodetect's header read).
    #[arg(long, default_value_t = 0)]
    skip: usize,
    /// Don't trim cell values.
    #[arg(long)]
    no_trim: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Delim {
    Csv,
    Tsv,
    Pipe,
}

pub async fn run(_ctx: &Ctx, args: Args) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let mut lines: Box<dyn Iterator<Item = std::io::Result<String>>> =
        Box::new(stdin.lock().lines());

    // honour --skip
    for _ in 0..args.skip {
        if lines.next().is_none() {
            break;
        }
    }

    let mut n_rows = 0usize;
    let mut n_skipped = 0usize;

    if args.passthrough {
        for line in lines {
            let line = line.context("reading stdin")?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(v) => {
                    writeln!(out, "{}", serde_json::to_string(&v)?)?;
                    n_rows += 1;
                }
                Err(_) => {
                    n_skipped += 1;
                    eprintln!("skip (invalid json): {}", truncate(&line, 80));
                }
            }
        }
    } else if let Some(pat) = args.regex.as_deref() {
        let re = Regex::new(pat).with_context(|| format!("compiling regex {pat:?}"))?;
        let names: Vec<Option<&str>> = re.capture_names().collect();
        for line in lines {
            let line = line.context("reading stdin")?;
            if line.trim().is_empty() {
                continue;
            }
            match re.captures(&line) {
                Some(caps) => {
                    let mut obj = Map::new();
                    for (i, n) in names.iter().enumerate() {
                        let value = caps.get(i).map(|m| m.as_str().to_owned()).unwrap_or_default();
                        let key = n
                            .map(|s| s.to_owned())
                            .unwrap_or_else(|| format!("col_{i}"));
                        if i == 0 && n.is_none() {
                            // skip the whole-match group
                            continue;
                        }
                        obj.insert(key, Value::String(value));
                    }
                    writeln!(out, "{}", serde_json::to_string(&Value::Object(obj))?)?;
                    n_rows += 1;
                }
                None => {
                    n_skipped += 1;
                }
            }
        }
    } else if let Some(d) = args.delim {
        let sep: char = match d {
            Delim::Csv => ',',
            Delim::Tsv => '\t',
            Delim::Pipe => '|',
        };
        let mut headers = args.headers.clone();
        let mut iter = lines.peekable();
        if headers.is_empty() && !args.no_header {
            if let Some(first) = iter.next() {
                let first = first.context("reading stdin")?;
                headers = first
                    .split(sep)
                    .map(|s| normalize_header(s, args.no_trim))
                    .collect();
            }
        }
        for line in iter {
            let line = line.context("reading stdin")?;
            if line.trim().is_empty() {
                continue;
            }
            let cells: Vec<String> = line.split(sep).map(|s| s.to_owned()).collect();
            let obj = pack_row(&headers, &cells, args.no_trim);
            writeln!(out, "{}", serde_json::to_string(&Value::Object(obj))?)?;
            n_rows += 1;
        }
    } else {
        // Column-header autodetect (default; ps -ef style).
        let lines_vec: Vec<String> = lines
            .map(|l| l.context("reading stdin"))
            .collect::<Result<Vec<_>>>()?;
        let mut row_lines = lines_vec.iter();
        let mut headers: Vec<String> = args.headers.clone();
        let mut col_starts: Vec<usize> = Vec::new();

        if headers.is_empty() && !args.no_header {
            let first = row_lines
                .find(|l| !l.trim().is_empty())
                .map(|s| s.to_owned());
            match first {
                Some(h) => {
                    let (hs, starts) = parse_header_row(&h);
                    headers = hs.into_iter().map(|s| normalize_header(&s, args.no_trim)).collect();
                    col_starts = starts;
                }
                None => return Ok(()),
            }
        }

        for line in row_lines {
            if line.trim().is_empty() {
                continue;
            }
            let cells: Vec<String> = if col_starts.is_empty() {
                line.split_whitespace().map(|s| s.to_owned()).collect()
            } else {
                slice_at_columns(line, &col_starts)
            };
            let obj = pack_row(&headers, &cells, args.no_trim);
            writeln!(out, "{}", serde_json::to_string(&Value::Object(obj))?)?;
            n_rows += 1;
        }
    }

    eprintln!("parsed {n_rows} rows, {n_skipped} skipped");
    Ok(())
}

fn pack_row(headers: &[String], cells: &[String], no_trim: bool) -> Map<String, Value> {
    let mut obj = Map::new();
    let n = headers.len().max(cells.len());
    for i in 0..n {
        let key = headers
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));
        let v = cells.get(i).cloned().unwrap_or_default();
        let v = if no_trim { v } else { v.trim().to_owned() };
        obj.insert(key, Value::String(v));
    }
    obj
}

fn normalize_header(s: &str, no_trim: bool) -> String {
    let s = if no_trim { s.to_owned() } else { s.trim().to_owned() };
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Parse a header row, returning the column names and the byte offsets where
/// each column starts. Headers are runs of non-whitespace.
fn parse_header_row(line: &str) -> (Vec<String>, Vec<usize>) {
    let mut headers = Vec::new();
    let mut starts = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // skip whitespace
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        while i < bytes.len() && !(bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        starts.push(start);
        headers.push(line[start..i].to_owned());
    }
    (headers, starts)
}

/// Slice a row at the given header start offsets. The last cell absorbs the rest.
fn slice_at_columns(line: &str, starts: &[usize]) -> Vec<String> {
    let mut cells = Vec::with_capacity(starts.len());
    let len = line.len();
    for w in starts.windows(2) {
        let (s, e) = (w[0], w[1]);
        if s >= len {
            cells.push(String::new());
        } else {
            let e = e.min(len);
            cells.push(line[s..e].to_owned());
        }
    }
    if let Some(&last) = starts.last() {
        if last <= len {
            cells.push(line[last..].to_owned());
        } else {
            cells.push(String::new());
        }
    }
    cells
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_owned()
    } else {
        format!("{}...", &s[..n])
    }
}
