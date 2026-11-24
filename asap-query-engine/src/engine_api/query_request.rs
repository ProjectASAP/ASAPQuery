use crate::data_model::QueryLanguage;

const MS_PER_SEC: f64 = 1000.0;

/// A parsed query, independent of the protocol it arrived on.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    pub language: QueryLanguage,
    pub query: String,
    pub eval: QueryEval,
}

/// When to evaluate a query. Times are unix epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEval {
    Instant {
        time_ms: i64,
    },
    Range {
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    },
}

/// Protocol seconds to milliseconds, rounded to the nearest millisecond.
///
/// Rounding (not truncation) makes `ms_to_secs(secs_to_ms(t)) == t` for every
/// millisecond-aligned `t`: e.g. `1095211880.820 * 1000.0` is
/// `1095211880819.9998`, which truncation would turn into ...819.
pub fn secs_to_ms(secs: f64) -> i64 {
    (secs * MS_PER_SEC).round() as i64
}

/// Milliseconds to protocol seconds.
pub fn ms_to_secs(ms: i64) -> f64 {
    ms as f64 / MS_PER_SEC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ms_aligned_seconds_round_trip_exactly() {
        for s in [
            "1700000000",
            "1700000000.1",
            "1700000000.123",
            "1095211880.820",
            "2189515850.298",
            "0.001",
            "60",
        ] {
            let secs: f64 = s.parse().unwrap();
            assert_eq!(ms_to_secs(secs_to_ms(secs)), secs, "{s}");
            assert_eq!(ms_to_secs(secs_to_ms(secs)).to_string(), secs.to_string());
        }
    }

    #[test]
    fn sub_millisecond_precision_rounds_to_nearest_ms() {
        assert_eq!(secs_to_ms(1700000000.1234), 1700000000123);
        assert_eq!(secs_to_ms(1700000000.1236), 1700000000124);
        assert_eq!(secs_to_ms(0.0004), 0);
    }
}
