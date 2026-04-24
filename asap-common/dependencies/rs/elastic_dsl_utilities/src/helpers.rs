/// Strip the `.keyword` suffix from a field name, if present.
pub fn strip_keyword_suffix(field: &str) -> &str {
    field.strip_suffix(".keyword").unwrap_or(field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_keyword_suffix_removes_only_trailing_keyword() {
        assert_eq!(strip_keyword_suffix("service.keyword"), "service");
        assert_eq!(strip_keyword_suffix("env"), "env");
    }
}
