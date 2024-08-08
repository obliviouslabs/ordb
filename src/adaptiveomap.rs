use crate::pageomap::PageOmap;

/**
 * A scalable map that creates an page omap when the size exceeds a threshold.
 */
pub struct AdaptiveOMap {
    pageoram: PageOmap,
    threshold: usize,
    size: usize,
}
