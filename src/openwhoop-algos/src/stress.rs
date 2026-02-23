use chrono::NaiveDateTime;
use whoop::{Activity, ParsedHistoryReading};

pub struct StressCalculator;

#[derive(Debug, Clone, Copy)]
pub struct StressScore {
    pub time: NaiveDateTime,
    /// 0.0 (no stress / well-recovered) to 10.0 (high stress / fatigued)
    pub score: f64,
    /// RMSSD of waking RR intervals in the last 4 hours (ms)
    pub waking_rmssd: f64,
    /// The sleep HRV baseline used for comparison, if available
    pub baseline_hrv: Option<f64>,
}

impl StressCalculator {
    /// Minimum number of waking readings required to compute a score
    pub const MIN_READINGS: usize = 60;
    /// Alias kept for backward compatibility with the Bluetooth device code path
    pub const MIN_READING_PERIOD: usize = Self::MIN_READINGS;

    /// Single-argument convenience wrapper (no sleep baseline).
    /// Used by the legacy Bluetooth/SQLite device flow in `openwhoop.rs`.
    pub fn calculate_stress_simple(hr: &[ParsedHistoryReading]) -> Option<StressScore> {
        Self::calculate_stress(hr, None)
    }
    /// How many hours of waking history to use
    pub const WAKING_WINDOW_HOURS: i64 = 4;

    /// Calculate a stress/recovery score from recent waking heart rate data.
    ///
    /// Algorithm:
    /// 1. Filter to the last 4 hours, excluding Active periods (exercise
    ///    elevates HR by design and should not be read as stress).
    /// 2. Build an RR interval sequence: use actual device RR intervals where
    ///    available, fall back to synthetic RR from BPM otherwise.
    /// 3. Compute RMSSD over that sequence.
    /// 4. If a sleep HRV baseline is provided, express the score as
    ///    `(1 - waking_rmssd / baseline) * 10` — a ratio near 1.0 means the
    ///    waking HRV matches sleep recovery levels (low stress), well below 1.0
    ///    means the autonomic system is under strain.
    ///    Without a baseline, fall back to an absolute RMSSD scale.
    pub fn calculate_stress(
        hr: &[ParsedHistoryReading],
        sleep_baseline_hrv: Option<f64>,
    ) -> Option<StressScore> {
        if hr.is_empty() {
            return None;
        }

        let latest_time = hr.last()?.time;
        let window_start = latest_time - chrono::Duration::hours(Self::WAKING_WINDOW_HOURS);

        // Filter to waking-rest readings in the last 4 hours (exclude Active/exercise)
        let waking_readings: Vec<&ParsedHistoryReading> = hr
            .iter()
            .filter(|r| r.time >= window_start)
            .filter(|r| !matches!(r.activity, Activity::Active))
            .collect();

        if waking_readings.len() < Self::MIN_READINGS {
            return None;
        }

        // Build an RR sequence.  For readings that have actual device RR
        // intervals, use them directly (multiple per reading is fine —
        // they're individual beat-to-beat intervals).  For readings without
        // RR data, synthesize one interval from BPM.
        let rr_sequence: Vec<f64> = waking_readings
            .iter()
            .flat_map(|r| {
                if !r.rr.is_empty() {
                    r.rr.iter().map(|&v| v as f64).collect::<Vec<_>>()
                } else if r.bpm > 0 {
                    vec![60.0 / r.bpm as f64 * 1000.0]
                } else {
                    vec![]
                }
            })
            .collect();

        if rr_sequence.len() < 2 {
            return None;
        }

        let waking_rmssd = Self::rmssd(&rr_sequence);

        let score = match sleep_baseline_hrv {
            Some(baseline) if baseline > 0.0 => {
                // Ratio-based: waking RMSSD relative to sleep HRV baseline.
                // ratio >= 1.0 → waking HRV at or above recovery level → score 0
                // ratio << 1.0 → waking HRV well below baseline → score approaches 10
                let ratio = waking_rmssd / baseline;
                ((1.0 - ratio).max(0.0) * 10.0).min(10.0)
            }
            _ => {
                // No baseline available: use absolute RMSSD scale.
                // Typical waking RMSSD: ~10 ms (high stress) to ~100+ ms (very relaxed).
                Self::absolute_score(waking_rmssd)
            }
        };

        Some(StressScore {
            time: latest_time,
            score,
            waking_rmssd,
            baseline_hrv: sleep_baseline_hrv,
        })
    }

    /// Root mean square of successive differences between consecutive RR intervals.
    fn rmssd(rr: &[f64]) -> f64 {
        if rr.len() < 2 {
            return 0.0;
        }
        let sum_sq: f64 = rr.windows(2).map(|w| (w[1] - w[0]).powi(2)).sum();
        (sum_sq / (rr.len() - 1) as f64).sqrt()
    }

    /// Map absolute RMSSD (ms) to a 0–10 stress scale when no sleep
    /// baseline is available.  10 ms → score 10, ≥100 ms → score 0.
    fn absolute_score(rmssd_ms: f64) -> f64 {
        let normalized = ((rmssd_ms - 10.0) / 90.0).clamp(0.0, 1.0);
        ((1.0 - normalized) * 10.0 * 100.0).round() / 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::StressCalculator;

    #[test]
    fn test_rmssd_constant_series() {
        // Constant RR → zero successive differences → RMSSD = 0
        let rr = vec![800.0; 10];
        assert_eq!(StressCalculator::rmssd(&rr), 0.0);
    }

    #[test]
    fn test_rmssd_alternating() {
        // Alternating 800/900 → each diff = 100 → RMSSD = 100
        let rr: Vec<f64> = (0..10).map(|i| if i % 2 == 0 { 800.0 } else { 900.0 }).collect();
        let result = StressCalculator::rmssd(&rr);
        assert!((result - 100.0).abs() < 0.001, "expected ~100, got {}", result);
    }

    #[test]
    fn test_absolute_score_bounds() {
        assert_eq!(StressCalculator::absolute_score(10.0), 10.0);  // floor
        assert_eq!(StressCalculator::absolute_score(100.0), 0.0);  // ceiling
        assert_eq!(StressCalculator::absolute_score(1000.0), 0.0); // clamped
    }
}
