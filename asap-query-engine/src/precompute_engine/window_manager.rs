/// Manages tumbling window boundaries and detects which windows have closed
/// based on watermark advancement.
pub struct WindowManager {
    /// Window size in milliseconds.
    window_size_ms: i64,
    /// Slide interval in milliseconds (== window_size_ms for tumbling windows).
    slide_interval_ms: i64,
}

impl WindowManager {
    /// Create a new WindowManager.
    ///
    /// `window_size_secs` and `slide_interval_secs` come from `AggregationConfig`
    /// (which stores them in seconds). They are converted to milliseconds internally.
    pub fn new(window_size_secs: u64, slide_interval_secs: u64) -> Self {
        let window_size_ms = (window_size_secs * 1000) as i64;
        let slide_interval_ms = if slide_interval_secs == 0 {
            window_size_ms // tumbling window
        } else {
            (slide_interval_secs * 1000) as i64
        };
        Self {
            window_size_ms,
            slide_interval_ms,
        }
    }

    pub fn window_size_ms(&self) -> i64 {
        self.window_size_ms
    }

    /// Compute the window start for a given timestamp.
    /// Windows are aligned to epoch (multiples of slide_interval_ms).
    pub fn window_start_for(&self, timestamp_ms: i64) -> i64 {
        // Floor-divide to the nearest slide interval boundary
        let n = timestamp_ms.div_euclid(self.slide_interval_ms);
        n * self.slide_interval_ms
    }

    /// Return window starts whose windows are now closed, given that the
    /// watermark advanced from `previous_wm` to `current_wm`.
    ///
    /// A window `[start, start + window_size_ms)` is closed when
    /// `current_wm >= start + window_size_ms`.
    ///
    /// Returns window starts in ascending order.
    pub fn closed_windows(&self, previous_wm: i64, current_wm: i64) -> Vec<i64> {
        if current_wm <= previous_wm || previous_wm == i64::MIN {
            // No watermark advancement, or first sample ever (nothing to close yet
            // — the window that contains the first sample is still open).
            return Vec::new();
        }

        let mut closed = Vec::new();

        // The earliest window start that *could* have been open at previous_wm.
        // A window is open if its end (start + window_size_ms) > previous_wm.
        // So the oldest open window start was: previous_wm - window_size_ms + 1,
        // aligned down to slide_interval.
        let earliest_open_start = self.window_start_for(
            (previous_wm - self.window_size_ms + 1).max(0),
        );

        let mut start = earliest_open_start;
        while start + self.window_size_ms <= current_wm {
            // This window was NOT closed at previous_wm but IS closed at current_wm
            if start + self.window_size_ms > previous_wm {
                closed.push(start);
            }
            start += self.slide_interval_ms;
        }

        closed
    }

    /// Return the window `[start, end)` boundaries for a given window start.
    pub fn window_bounds(&self, window_start: i64) -> (i64, i64) {
        (window_start, window_start + self.window_size_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window_start() {
        // 60-second (60000ms) tumbling windows
        let wm = WindowManager::new(60, 0);

        assert_eq!(wm.window_start_for(0), 0);
        assert_eq!(wm.window_start_for(59_999), 0);
        assert_eq!(wm.window_start_for(60_000), 60_000);
        assert_eq!(wm.window_start_for(119_999), 60_000);
        assert_eq!(wm.window_start_for(120_000), 120_000);
    }

    #[test]
    fn test_no_closed_windows_on_first_sample() {
        let wm = WindowManager::new(60, 0);
        let closed = wm.closed_windows(i64::MIN, 30_000);
        assert!(closed.is_empty());
    }

    #[test]
    fn test_tumbling_window_close() {
        // 60s tumbling windows
        let wm = WindowManager::new(60, 0);

        // Watermark advances from 30_000 to 70_000
        // Window [0, 60_000) closes when wm >= 60_000
        let closed = wm.closed_windows(30_000, 70_000);
        assert_eq!(closed, vec![0]);
    }

    #[test]
    fn test_multiple_window_closes() {
        // 10s (10000ms) tumbling windows
        let wm = WindowManager::new(10, 0);

        // Watermark jumps from 5_000 to 35_000 — closes windows 0, 10_000, 20_000
        let closed = wm.closed_windows(5_000, 35_000);
        assert_eq!(closed, vec![0, 10_000, 20_000]);
    }

    #[test]
    fn test_no_close_when_watermark_stagnant() {
        let wm = WindowManager::new(60, 0);
        let closed = wm.closed_windows(30_000, 30_000);
        assert!(closed.is_empty());
    }

    #[test]
    fn test_window_bounds() {
        let wm = WindowManager::new(60, 0);
        assert_eq!(wm.window_bounds(0), (0, 60_000));
        assert_eq!(wm.window_bounds(60_000), (60_000, 120_000));
    }

    #[test]
    fn test_sliding_window() {
        // 30s window, 10s slide
        let wm = WindowManager::new(30, 10);

        assert_eq!(wm.window_start_for(0), 0);
        assert_eq!(wm.window_start_for(9_999), 0);
        assert_eq!(wm.window_start_for(10_000), 10_000);

        // Watermark advances from 15_000 to 35_000
        // Window [0, 30_000) closes at wm=30_000 (was open at 15_000)
        let closed = wm.closed_windows(15_000, 35_000);
        assert_eq!(closed, vec![0]);
    }
}
