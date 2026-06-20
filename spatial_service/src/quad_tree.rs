pub struct Rect {
    pub x: f32,
    pub y: f32,
    pub w: f32,
    pub h: f32,
}

impl Rect {
    fn contains(&self, pos: [f32; 2]) -> bool {
        pos[0] >= self.x && pos[0] < self.x + self.w
            && pos[1] >= self.y && pos[1] < self.y + self.h
    }

    // Returns true if this rect overlaps the square [cx-m, cx+m] x [cy-m, cy+m].
    fn intersects_margin(&self, pos: [f32; 2], margin: f32) -> bool {
        let [cx, cy] = pos;
        self.x < cx + margin && self.x + self.w > cx - margin
            && self.y < cy + margin && self.y + self.h > cy - margin
    }
}

pub struct QuadTree {
    pub bounds: Rect,
    depth: u8,
    max_depth: u8,
    children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: Option<u32>,
}

impl QuadTree {
    pub fn new(bounds: Rect, depth: u8, max_depth: u8, shard_id: Option<u32>) -> Self {
        Self { bounds, depth, max_depth, children: None, shard_id }
    }

    pub fn split(&mut self, shard_ids: [u32; 4]) {
        let hw = self.bounds.w / 2.0;
        let hh = self.bounds.h / 2.0;
        let x = self.bounds.x;
        let y = self.bounds.y;
        let d = self.depth + 1;
        let md = self.max_depth;

        self.children = Some(Box::new([
            QuadTree::new(Rect { x,      y,      w: hw, h: hh }, d, md, Some(shard_ids[0])),
            QuadTree::new(Rect { x: x+hw, y,      w: hw, h: hh }, d, md, Some(shard_ids[1])),
            QuadTree::new(Rect { x,      y: y+hh, w: hw, h: hh }, d, md, Some(shard_ids[2])),
            QuadTree::new(Rect { x: x+hw, y: y+hh, w: hw, h: hh }, d, md, Some(shard_ids[3])),
        ]));
        self.shard_id = None;
    }

    pub fn shard_for(&self, pos: [f32; 2]) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                if let Some(id) = child.shard_for(pos) {
                    return Some(id);
                }
            }
            return None;
        }
        self.shard_id
    }

    pub fn shards_near(&self, pos: [f32; 2], margin: f32) -> Vec<u32> {
        let mut result = Vec::new();
        self.collect_shards_near(pos, margin, &mut result);
        result.sort_unstable();
        result.dedup();
        result
    }

    fn collect_shards_near(&self, pos: [f32; 2], margin: f32, out: &mut Vec<u32>) {
        if !self.bounds.intersects_margin(pos, margin) {
            return;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                child.collect_shards_near(pos, margin, out);
            }
        } else if let Some(id) = self.shard_id {
            out.push(id);
        }
    }
}

/// Default world: 1000x1000, split into 4 quadrants assigned to shards 0–3.
pub fn build_default() -> QuadTree {
    let mut tree = QuadTree::new(
        Rect { x: 0.0, y: 0.0, w: 1000.0, h: 1000.0 },
        0,
        1,
        None,
    );
    tree.split([0, 1, 2, 3]);
    tree
}

#[cfg(test)]
mod tests {
    use super::*;

    // Layout produced by build_default():
    //   Shard 0: x∈[0,500)   y∈[0,500)     top-left
    //   Shard 1: x∈[500,1000) y∈[0,500)    top-right
    //   Shard 2: x∈[0,500)   y∈[500,1000)  bottom-left
    //   Shard 3: x∈[500,1000) y∈[500,1000) bottom-right

    fn t() -> QuadTree { build_default() }

    // ── shard_for: quadrant centers ────────────────────────────────────────────

    #[test]
    fn shard_for_center_of_each_quadrant() {
        let t = t();
        assert_eq!(t.shard_for([250.0, 250.0]), Some(0));
        assert_eq!(t.shard_for([750.0, 250.0]), Some(1));
        assert_eq!(t.shard_for([250.0, 750.0]), Some(2));
        assert_eq!(t.shard_for([750.0, 750.0]), Some(3));
    }

    // ── shard_for: world corners ───────────────────────────────────────────────

    #[test]
    fn shard_for_world_corners() {
        let t = t();
        assert_eq!(t.shard_for([0.0,   0.0  ]), Some(0));
        assert_eq!(t.shard_for([999.9, 0.0  ]), Some(1));
        assert_eq!(t.shard_for([0.0,   999.9]), Some(2));
        assert_eq!(t.shard_for([999.9, 999.9]), Some(3));
    }

    // ── shard_for: exact boundary lines ───────────────────────────────────────
    // contains() uses >=left and <right, so x=500 falls in the right shard.

    #[test]
    fn shard_for_exact_vertical_boundary() {
        let t = t();
        assert_eq!(t.shard_for([500.0, 250.0]), Some(1)); // right side
        assert_eq!(t.shard_for([499.9, 250.0]), Some(0)); // still left
        assert_eq!(t.shard_for([500.0, 750.0]), Some(3)); // right-bottom
    }

    #[test]
    fn shard_for_exact_horizontal_boundary() {
        let t = t();
        assert_eq!(t.shard_for([250.0, 500.0]), Some(2)); // bottom side
        assert_eq!(t.shard_for([250.0, 499.9]), Some(0)); // still top
        assert_eq!(t.shard_for([750.0, 500.0]), Some(3)); // right-bottom
    }

    #[test]
    fn shard_for_exact_corner_point() {
        // (500,500) — right-bottom → shard 3
        let t = t();
        assert_eq!(t.shard_for([500.0, 500.0]), Some(3));
    }

    // ── shard_for: out of bounds ───────────────────────────────────────────────

    #[test]
    fn shard_for_out_of_bounds() {
        let t = t();
        assert_eq!(t.shard_for([-1.0,  250.0 ]), None);
        assert_eq!(t.shard_for([250.0, -1.0  ]), None);
        assert_eq!(t.shard_for([1000.0, 250.0]), None); // exclusive upper bound
        assert_eq!(t.shard_for([250.0, 1000.0]), None);
        assert_eq!(t.shard_for([1500.0, 1500.0]), None);
    }

    // ── shards_near: basic cases ───────────────────────────────────────────────

    #[test]
    fn shards_near_deep_in_quadrant_returns_one() {
        let t = t();
        assert_eq!(t.shards_near([250.0, 250.0], 50.0), vec![0]);
        assert_eq!(t.shards_near([750.0, 750.0], 50.0), vec![3]);
    }

    // x=460, margin=50 → x+margin=510 > 500 → shard 1 intersects
    #[test]
    fn shards_near_vertical_boundary_left_side() {
        let t = t();
        assert_eq!(t.shards_near([460.0, 250.0], 50.0), vec![0, 1]);
    }

    // x=540, margin=50 → x-margin=490 < 500 → shard 0 boundary intersects
    #[test]
    fn shards_near_vertical_boundary_right_side() {
        let t = t();
        assert_eq!(t.shards_near([540.0, 250.0], 50.0), vec![0, 1]);
    }

    #[test]
    fn shards_near_horizontal_boundary() {
        let t = t();
        assert_eq!(t.shards_near([250.0, 460.0], 50.0), vec![0, 2]);
        assert_eq!(t.shards_near([250.0, 540.0], 50.0), vec![0, 2]);
    }

    // Near corner: all 4 shards within margin
    #[test]
    fn shards_near_corner_all_four() {
        let t = t();
        assert_eq!(t.shards_near([490.0, 490.0], 50.0), vec![0, 1, 2, 3]);
        assert_eq!(t.shards_near([510.0, 510.0], 50.0), vec![0, 1, 2, 3]);
    }

    // Exact margin distance: intersects_margin uses strict <, so boundary is NOT crossed.
    #[test]
    fn shards_near_exactly_at_margin_distance_not_triggered() {
        let t = t();
        // x=450, margin=50: 450+50=500, shard1 requires self.x(500) < 500 → FALSE
        assert_eq!(t.shards_near([450.0, 250.0], 50.0), vec![0]);
    }

    // One unit inside the margin: does trigger
    #[test]
    fn shards_near_one_unit_inside_margin() {
        let t = t();
        assert_eq!(t.shards_near([451.0, 250.0], 50.0), vec![0, 1]);
    }

    // Position exactly on boundary line
    #[test]
    fn shards_near_on_boundary_line() {
        let t = t();
        // x=500 is in shard 1; with margin=50, shard 0 (which ends at 500) is also near
        assert_eq!(t.shards_near([500.0, 250.0], 50.0), vec![0, 1]);
    }

    // At world edge: no phantom shard to the left
    #[test]
    fn shards_near_at_world_left_edge() {
        let t = t();
        assert_eq!(t.shards_near([10.0, 250.0], 50.0), vec![0]);
    }

    #[test]
    fn shards_near_at_world_top_edge() {
        let t = t();
        assert_eq!(t.shards_near([250.0, 10.0], 50.0), vec![0]);
    }
}

// Property-based tests: instead of picking positions by hand, check invariants
// that must hold for *every* point in the world. Catches edge cases the
// example tests above might miss (e.g. near boundaries with arbitrary margins).
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    // Interior positions, kept strictly inside [0,1000) so shard_for is defined.
    fn in_bounds() -> impl Strategy<Value = [f32; 2]> {
        (0.0f32..999.0, 0.0f32..999.0).prop_map(|(x, y)| [x, y])
    }

    proptest! {
        // Any in-bounds point belongs to exactly one of the four shards.
        #[test]
        fn shard_for_is_defined_inside_world(pos in in_bounds()) {
            let id = build_default().shard_for(pos);
            prop_assert!(matches!(id, Some(0..=3)));
        }

        // The owning shard is always part of its own neighbourhood.
        #[test]
        fn owning_shard_is_always_near(pos in in_bounds(), margin in 0.0f32..500.0) {
            let t = build_default();
            let owner = t.shard_for(pos).unwrap();
            prop_assert!(t.shards_near(pos, margin).contains(&owner));
        }

        // A wider margin can only add shards, never remove them.
        #[test]
        fn shards_near_is_monotonic_in_margin(pos in in_bounds(), m in 0.0f32..400.0) {
            let t = build_default();
            let small = t.shards_near(pos, m);
            let large = t.shards_near(pos, m + 50.0);
            for s in &small {
                prop_assert!(large.contains(s));
            }
        }

        // Results are always sorted, deduped and within the valid shard range.
        #[test]
        fn shards_near_output_is_well_formed(pos in in_bounds(), margin in 0.0f32..600.0) {
            let near = build_default().shards_near(pos, margin);
            let mut sorted = near.clone();
            sorted.sort_unstable();
            sorted.dedup();
            prop_assert_eq!(near.clone(), sorted);
            prop_assert!(near.iter().all(|&s| s <= 3));
        }
    }
}
