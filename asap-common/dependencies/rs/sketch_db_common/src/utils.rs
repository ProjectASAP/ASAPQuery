/// Normalize spatial filter for PromQL queries
pub fn normalize_spatial_filter(filter: &str) -> String {
    if filter.is_empty() {
        return String::new();
    }

    // TODO: Parse the spatial filter, make fake ASTs, each one with matcher,
    // prettify each, and sort them. Unfortunately, unable to manually create fake ASTs
    // Current workaround: split spatial filter by commas, sort, and join

    let trimmed = filter.trim().strip_prefix('{').unwrap_or(filter.trim());
    let trimmed = trimmed.strip_suffix('}').unwrap_or(trimmed);
    let trimmed = trimmed.trim();

    let mut parts: Vec<&str> = trimmed.split(',').collect();
    parts.sort();

    format!("{{{}}}", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_spatial_filter() {
        assert_eq!(normalize_spatial_filter("").as_str(), "");

        let result = normalize_spatial_filter("instance=\"localhost:9090\"");
        assert_eq!(result, "{instance=\"localhost:9090\"}");

        let result = normalize_spatial_filter("{instance=\"localhost:9090\"}");
        assert_eq!(result, "{instance=\"localhost:9090\"}");

        let result = normalize_spatial_filter("{job=\"prometheus\",instance=\"localhost:9090\"}");
        assert_eq!(result, "{instance=\"localhost:9090\",job=\"prometheus\"}");

        let result = normalize_spatial_filter("job=\"prometheus\",instance=\"localhost:9090\"");
        assert_eq!(result, "{instance=\"localhost:9090\",job=\"prometheus\"}");
    }
}
