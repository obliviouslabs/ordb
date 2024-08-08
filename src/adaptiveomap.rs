use crate::flexomap::FlexOmap;

/**
 * A scalable map that creates an page omap when the size exceeds a threshold.
 */
pub struct AdaptiveOMap {
    flexoram: FlexOmap,
    threshold: usize,
    size: usize,
}
