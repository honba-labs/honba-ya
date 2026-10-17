//! Lot-aligned sizing and freeze-quantity order slicing.
//!
//! Indian F&O trades in whole *lots*, and an exchange *freeze* quantity caps how much a single
//! order may carry. [`lot_aligned_size`] floors a requested quantity to a whole number of lots
//! (never overshooting the request); [`split_order`] slices a lot-aligned quantity into a
//! sequence of clips that are each no larger than the freeze and each a whole number of lots, so
//! a large order can be worked as several exchange-legal orders whose fills aggregate to the
//! same size.

/// Quantities closer than this are equal (float residue).
const QTY_EPS: f64 = 1e-9;

/// The largest whole number of lots that does not exceed `quantity`.
///
/// The request floors towards zero, so the result never overshoots it. A request below one lot,
/// a non-finite `quantity`, or a non-positive/non-finite `lot_size` is `0.0`.
pub fn lot_aligned_size(quantity: f64, lot_size: f64) -> f64 {
    if !quantity.is_finite() || !(lot_size.is_finite() && lot_size > 0.0) || quantity <= 0.0 {
        return 0.0;
    }
    (quantity / lot_size).floor() * lot_size
}

/// Slices `quantity` into lot-aligned clips, each no larger than `freeze_qty`.
///
/// The requested quantity floors to whole lots first ([`lot_aligned_size`]); the largest
/// lot-aligned clip that fits the freeze is then taken repeatedly, and the final lot-aligned
/// remainder closes the sequence. Every clip is a multiple of `lot_size` and `<= freeze_qty`,
/// and the clips sum to `lot_aligned_size(quantity, lot_size)`. Returns an empty vector when the
/// request is below one lot, or when the freeze is smaller than one lot (no legal clip exists).
///
/// A non-positive or non-finite `freeze_qty` means no cap: the whole lot-aligned quantity is a
/// single clip.
pub fn split_order(quantity: f64, lot_size: f64, freeze_qty: f64) -> Vec<f64> {
    let total = lot_aligned_size(quantity, lot_size);
    if total <= QTY_EPS {
        return Vec::new();
    }
    if !(freeze_qty.is_finite() && freeze_qty > 0.0) {
        return vec![total];
    }
    let clip = lot_aligned_size(freeze_qty, lot_size);
    if clip <= QTY_EPS {
        return Vec::new();
    }
    let mut clips = Vec::new();
    let mut remaining = total;
    while remaining > clip + QTY_EPS {
        clips.push(clip);
        remaining -= clip;
    }
    if remaining > QTY_EPS {
        clips.push(remaining);
    }
    clips
}
