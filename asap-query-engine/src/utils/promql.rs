pub use asap_types::utils::normalize_spatial_filter;

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
