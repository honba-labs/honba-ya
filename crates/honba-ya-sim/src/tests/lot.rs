//! Unit tests for `crate::lot`: lot-aligned sizing and freeze-quantity slicing.

use crate::lot::{lot_aligned_size, split_order};

#[test]
fn slice_above_freeze() {
    // 500 with lot 75 / freeze 150: the request floors to 6 lots (450) and slices into three
    // freeze-capped clips.
    let clips = split_order(500.0, 75.0, 150.0);
    assert_eq!(clips, vec![150.0, 150.0, 150.0]);
    assert_eq!(lot_aligned_size(500.0, 75.0), 450.0);
    assert_eq!(clips.iter().sum::<f64>(), lot_aligned_size(500.0, 75.0));
    for clip in &clips {
        assert!(*clip <= 150.0);
        assert_eq!(clip % 75.0, 0.0);
    }

    // A freeze that is not a lot multiple caps each clip at the largest lot-aligned size.
    assert_eq!(
        split_order(300.0, 75.0, 100.0),
        vec![75.0, 75.0, 75.0, 75.0]
    );

    // A non-aligned request floors to the nearest lot before slicing.
    assert_eq!(lot_aligned_size(200.0, 75.0), 150.0);
    assert_eq!(split_order(200.0, 75.0, 150.0), vec![150.0]);

    // A lot-aligned remainder smaller than the cap closes the sequence.
    assert_eq!(split_order(400.0, 75.0, 150.0), vec![150.0, 150.0, 75.0]);

    // Below one lot: nothing to trade.
    assert_eq!(lot_aligned_size(50.0, 75.0), 0.0);
    assert!(split_order(50.0, 75.0, 150.0).is_empty());
}
