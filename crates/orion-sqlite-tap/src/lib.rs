//! Pure helpers for the SQLite CDC tap.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CdcEvent {
    pub at: String,
    pub table: String,
    pub rowid: i64,
    pub row: serde_json::Value,
    pub _subject: String,
}

/// Build the SQL the tap runs every tick — selects rows with rowid > since,
/// ordered ascending. Caller is responsible for validating table + columns
/// against a safe-charset (table names from env are still trusted-input from
/// the operator).
pub fn build_select(table: &str, since: i64) -> String {
    format!("SELECT rowid, * FROM \"{table}\" WHERE rowid > {since} ORDER BY rowid ASC LIMIT 1000")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_select_pages_after_cursor() {
        let sql = build_select("events", 100);
        assert!(sql.contains("FROM \"events\""));
        assert!(sql.contains("rowid > 100"));
        assert!(sql.contains("ORDER BY rowid ASC"));
        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn build_select_handles_zero_cursor() {
        let sql = build_select("items", 0);
        assert!(sql.contains("rowid > 0"));
    }

    #[test]
    fn build_select_quotes_table_to_allow_kebab_and_unusual_names() {
        let sql = build_select("user-events", 0);
        assert!(sql.contains("\"user-events\""));
    }
}
