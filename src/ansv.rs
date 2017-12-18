use std;
use rayon;
use rayon::prelude::*;
use utils;

#[inline(always)]
fn arr_length_at_depth(base_length: usize, depth: usize) -> usize {
    ((base_length as f64)/(2f64.powi(depth as i32))).ceil() as usize
}

#[inline(always)]
fn left(i: usize) -> usize {
    i << 1
}

#[inline(always)]
fn right(i: usize) -> usize {
    (i << 1) | 1
}

#[inline(always)]
fn parent(i: usize) -> usize {
    i >> 1
}

#[derive(Debug, Clone)]
struct ArrayTree<T> {
    layers: Box<[Box<[T]>]>
}
struct ArrayTreeView<'a, T: 'a> {
    tree: &'a ArrayTree<T>,
    view: Box<[&'a [T]]>,
    pub cur_idx: usize,
    pub cur_depth: usize
}

// Can't keep any reference to the original tree
// while holding mutable borrows to its layers.
struct ArrayTreeViewMut<'a, T: 'a> {
    view: Box<[&'a mut [T]]>,
    pub cur_idx: usize,
    pub cur_depth: usize
}
impl<'a, T: 'a> ArrayTreeView<'a, T> {
    fn new(base: &'a [T], tree: &'a ArrayTree<T>) -> ArrayTreeView<'a, T> {
        let mut view = Vec::<&[T]>::with_capacity(tree.depth());
        view.push(base);

        for layer in tree.layers().iter() {
            view.push(layer.as_ref());
        }

        ArrayTreeView {
            tree: tree,
            view: view.into_boxed_slice(),
            cur_idx: 0,
            cur_depth: 0
        }
    }

    #[inline]
    fn depth(&self) -> usize {self.tree.depth()}

    #[inline]
    fn as_table(&self) -> &Box<[&[T]]> {
        &self.view
    }

    #[inline]
    fn go_to_parent(&mut self){
        self.cur_depth += 1;
        self.cur_idx = parent(self.cur_idx);
    }

    #[inline]
    fn go_to_left_child(&mut self) {
        self.cur_depth -= 1;
        self.cur_idx = left(self.cur_idx);
    }

    #[inline]
    fn go_to_right_child(&mut self) {
        self.cur_depth -= 1;
        self.cur_idx = right(self.cur_idx);
    }

    #[inline]
    fn go_to(&mut self, depth: usize, idx: usize) {
        self.go_to_depth(depth);
        self.go_to_idx(idx);
    }

    #[inline]
    fn go_to_idx(&mut self, idx: usize) {
        self.cur_idx = idx;
    }

    #[inline]
    fn go_to_depth(&mut self, depth: usize) {
        self.cur_depth = depth;
    }

    #[inline]
    fn val(&self) -> &T {
        &self.view[self.cur_depth][self.cur_idx]
    }

    #[inline]
    fn val_at(&self, depth: usize, idx: usize) -> &T {
        &self.view[depth][idx]
    }

    #[inline]
    fn val_at_right_child(&self) -> &T {
        &self.view[self.cur_depth-1][right(self.cur_idx)]
    }

    #[inline]
    fn val_at_left_child(&self) -> &T {
        &self.view[self.cur_depth-1][left(self.cur_idx)]
    }

    #[inline]
    fn go_to_bottom(&mut self) {
        self.cur_depth = self.depth() - 1;
    }

    #[inline]
    fn max_width(&self) -> usize {
        self.view[0].len()
    }

    #[inline]
    fn cur_width(&self) -> usize {
        self.view[self.cur_depth].len()
    }

}
impl<'a, T: 'a> ArrayTreeViewMut<'a, T> {
    fn new(base: &'a mut [T], tree: &'a mut ArrayTree<T>) -> ArrayTreeViewMut<'a, T> {
        let mut view = Vec::<&mut [T]>::with_capacity(tree.depth());
        view.push(base);

        for layer in tree.layers_mut().iter_mut() {
            view.push(layer.as_mut());
        }

        ArrayTreeViewMut {
            view: view.into_boxed_slice(),
            cur_idx: 0,
            cur_depth: 0
        }
    }
    fn depth(&self) -> usize {self.view.len()}

    fn as_table_mut(&mut self) -> &mut Box<[&'a mut [T]]> {
        &mut self.view
    }
}

impl<T> ArrayTree<T> {
    fn new(base: &[T]) -> ArrayTree<T> where T: Default + Clone {
        let l2 = (base.len() as f64).log2().ceil() as usize;
        let depth = l2 + 1;
        let mut table_arrs = (1..depth).map(|idx| {
            vec![Default::default(); arr_length_at_depth(base.len(), idx)].into_boxed_slice()
        }).collect::<Vec<Box<[T]>>>().into_boxed_slice();
        ArrayTree {
            layers: table_arrs
        }
    }
    #[inline(always)]
    fn depth(&self) -> usize {
        1 + self.layers.len()
    }
    fn layers(&self) -> &Box<[Box<[T]>]> {
        & self.layers
    }
    pub fn layers_mut(&mut self) -> &mut Box<[Box<[T]>]> {
        &mut self.layers
    }
}


fn get_left_opt( tree_view: &mut ArrayTreeView<usize>, real_idx: usize, start: usize) -> isize {
    let value = *tree_view.val_at(0, real_idx);
    tree_view.go_to_bottom();
    tree_view.go_to_idx(0);
    if value == *tree_view.val() {
        return -1;
    }

    tree_view.go_to(0, start);
    tree_view.go_to_parent();

    let mut dist = 2; // Just 2^depth
    for _ in tree_view.cur_depth..tree_view.depth() {
        if (tree_view.cur_idx + 1)*dist > real_idx + 1 { // Are we potentially beyond the real_idx now?
            if tree_view.cur_idx == 0 {
                return -1
            } else {
                tree_view.cur_idx -= 1;
            }
        }

        if *tree_view.val() >= value {
            tree_view.go_to_parent();
        } else {
            break;
        }

        dist <<= 1;
    }

    while tree_view.cur_depth > 0 {
        if *tree_view.val_at_right_child() < value {
            tree_view.go_to_right_child();
        } else {
            tree_view.go_to_left_child();
        }
    }
    tree_view.cur_idx as isize
}

fn get_right_opt( tree_view: &mut ArrayTreeView<usize>, real_idx: usize, start: usize) -> isize {
    let value = *tree_view.val_at(0, real_idx);
    tree_view.go_to_bottom();
    tree_view.go_to_idx(0);
    if value == *tree_view.val() {
        return -1;
    }

    tree_view.go_to(0, start);
    tree_view.go_to_parent();

    let mut dist = 2; // Just 2^depth
    while tree_view.cur_depth < tree_view.depth() {
        if tree_view.cur_idx*dist < real_idx { // Are we potentially beyond the real_idx now?
            if tree_view.cur_idx == tree_view.cur_width() - 1 {
                return -1
            } else {
                tree_view.cur_idx += 1;
            }
        }

        if *tree_view.val() >= value {
            tree_view.go_to_parent();
        } else {
            break;
        }

        dist <<= 1;
    }

    while tree_view.cur_depth > 0 {
        if *tree_view.val_at_left_child() < value {
            tree_view.go_to_left_child();
        } else {
            tree_view.go_to_right_child();
        }
    }
    tree_view.cur_idx as isize
}

fn compute_ansv_linear(indices: &[usize], left_nearest_neighbors: &mut[isize], right_nearest_neighbors: &mut [isize], offset: usize) {
    let mut unsafe_stack = utils::UncheckedFixedSizeStack::<usize>::new(indices.len());

    // depends on state of stack -- only works in serial
    let mut get_nearest_neighbor = |stack: &mut utils::UncheckedFixedSizeStack<usize>, idx, dest: &mut isize| {
        while stack.len() > 0 && indices[unsafe {*stack.peek()}] > indices[idx] {
            unsafe {
                stack.pop();
            }
        }
        if stack.len() == 0 {
            *dest = -1;
        } else {
            unsafe {
                *dest = (*stack.peek() + offset) as isize;
            }
        }
        unsafe {
            stack.push(idx);
        }
    };
    left_nearest_neighbors.iter_mut().enumerate().for_each(|(idx, val)| get_nearest_neighbor(&mut unsafe_stack, idx, val));
    unsafe_stack.clear();
    right_nearest_neighbors.iter_mut().enumerate().rev().for_each(|(idx, val)| get_nearest_neighbor(&mut unsafe_stack,idx, val));
}

fn construct_min_search_tree(base: &[usize]) -> ArrayTree<usize> {
    let mut min_tree = ArrayTree::<usize>::new(base);

    let mut update_row = |child_row: &[usize], cur_row: &mut[usize]| {
        let skip_end = if child_row.len() % 2 == 1 {
            1
        } else {
            0
        };
        let cur_len = cur_row.len();
        cur_row.par_iter_mut().enumerate().rev().skip(skip_end).for_each(|(idx, v)| {
            *v = child_row[left(idx)].min(child_row[right(idx)]);
        });
        if skip_end == 1 {
            *cur_row.last_mut().unwrap() = *child_row.last().unwrap();
        }
    };

    {
        let depth = min_tree.depth();
        let mut other_rows = min_tree.layers_mut();

        update_row(base, other_rows.first_mut().unwrap().as_mut());

        for d in 2..depth {
            let (mut before, mut cur, mut after) = utils::extract_at_mut(other_rows, d-1);
            update_row(before.last().unwrap().as_ref(), cur.as_mut());
        }
    }

    min_tree

}

pub fn compute_ansv(indices: &mut [usize]) -> (Box<[isize]>, Box<[isize]>) {
    let indices_len = indices.len();

    let mut left_nearest_neighbors = vec![0isize; indices_len].into_boxed_slice();
    let mut right_nearest_neighbors = vec![0isize; indices_len].into_boxed_slice();

    let min_tree = construct_min_search_tree(indices);

    let chunk_size = utils::rayon_chunk_size(indices_len);

    generic_izip!(indices.par_chunks(chunk_size),
        left_nearest_neighbors.par_chunks_mut(chunk_size),
        right_nearest_neighbors.par_chunks_mut(chunk_size)).enumerate().for_each(
        |(idx, (indices_chunk, lnn_chunk, rnn_chunk)): (usize, (&[usize], &mut [isize], &mut [isize]))| {
            let mut tree_view = ArrayTreeView::new(indices, &min_tree);

            compute_ansv_linear(indices_chunk, lnn_chunk, rnn_chunk, idx * chunk_size);

            lnn_chunk.iter_mut().enumerate().fold((idx*chunk_size) as isize, |mut chunk_lnn_idx, (sub_idx, lnn_val)| {
                if *lnn_val == -1 {
                    let abs_idx = sub_idx + idx * chunk_size;
                    // If chunk_lnn_idx == -1 there are no smaller nearest neighbors anywhere outside of this chunk either.
                    // Also, only update the chunk_lnn_idx if the value at that position is no longer smaller than the value we're looking at.
                    if chunk_lnn_idx != -1 && indices[chunk_lnn_idx as usize] >= indices[abs_idx] {
                        chunk_lnn_idx = get_left_opt(&mut tree_view, abs_idx, chunk_lnn_idx as usize);
                    }
                    *lnn_val = chunk_lnn_idx;
                }
                chunk_lnn_idx
            });

            rnn_chunk.iter_mut().enumerate().rev().fold((idx*chunk_size + indices_chunk.len() - 1) as isize, |mut chunk_rnn_idx, (sub_idx, rnn_val)| {
                if *rnn_val == -1 {
                    let abs_idx = sub_idx + idx * chunk_size;
                    // If chunk_lnn_idx == -1 there are no smaller nearest neighbors anywhere outside of this chunk either.
                    // Also, only update the chunk_lnn_idx if the value at that position is no longer smaller than the value we're looking at.
                    if chunk_rnn_idx != -1 && indices[chunk_rnn_idx as usize] >= indices[abs_idx] {
                        chunk_rnn_idx = get_right_opt(&mut tree_view, abs_idx, chunk_rnn_idx as usize);
                    }
                    *rnn_val = chunk_rnn_idx;
                }
                chunk_rnn_idx
            });
        }
    );

    (left_nearest_neighbors, right_nearest_neighbors)
}

#[cfg(test)]
mod test {
    use std;
    use utils;
    use rand;
    use rand::Rng;
    use rayon::prelude::*;
    use test;
    #[test]
    fn arr_length_at_depth_matches_source() {
        let depth = (utils::DEFAULT_TEST_SIZE as f64).log2().ceil() as usize;
        for mut m in 1..utils::DEFAULT_TEST_SIZE {
            let v1 = (1..depth).map(|idx| {
                super::arr_length_at_depth(m, idx)
            }).collect::<Vec<usize>>();
            let mut v2 = Vec::<usize>::with_capacity(depth - 1);
            for idx in 1..depth {
                m = (m + 1)/2;
                v2.push(m);
            }
            assert_eq!(v1, v2);
        }
    }
    #[bench]
    fn ansv_bench(bench: &mut test::Bencher) {
        let mut data = (0usize..utils::BENCH_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());
        bench.iter(|| {
            super::compute_ansv(data.as_mut());
        })
    }
    #[bench]
    fn construct_min_search_tree_bench(bench: &mut test::Bencher) {
        let mut data = (0usize..utils::BENCH_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());
        bench.iter(|| {
            super::construct_min_search_tree(data.as_mut());
        })
    }
    #[bench]
    fn get_left_opt_bench(bench: &mut test::Bencher) {
        let mut data = (0usize..utils::BENCH_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());
        let tree = super::construct_min_search_tree(data.as_mut());
        bench.iter(|| {
            let mut tree_view = super::ArrayTreeView::new(data.as_ref(), &tree);
            let idx = rng.gen_range::<usize>(0, data.len());
            super::get_left_opt(&mut tree_view, idx, idx);
        })
    }

    fn validate_ansv(data: &[usize], lnn: &[isize], rnn: &[isize]) {
        lnn.iter().enumerate().for_each(|(idx, &lnn_idx)| {
            let mut scan_pos : usize;
            if lnn_idx != -1 {
                assert!(data[idx] > data[lnn_idx as usize]);
                scan_pos = (lnn_idx + 1) as usize;
            } else {
                scan_pos = 0;
            }
            data[scan_pos..idx].iter().enumerate().for_each(|(bv_idx, &between_value)| {
                if data[idx] > between_value {
                    dbg!(idx, lnn_idx, bv_idx, data[idx], data[lnn_idx as usize], between_value);
                }
                assert!(data[idx] <= between_value);
            })
        });
        rnn.iter().enumerate().for_each(|(idx, &rnn_idx)| {
            let mut scan_pos: usize;
            if rnn_idx != -1 {
                assert!(data[idx] > data[rnn_idx as usize]);
                scan_pos = rnn_idx as usize;
            } else {
                scan_pos = data.len();
            }
            data[idx..scan_pos].iter().enumerate().for_each(|(bv_idx, &between_value)| {
                assert!(data[idx] <= between_value);
            })
        });

    }

    #[test]
    fn min_search_tree_test() {
        let mut data = (0usize..utils::DEFAULT_TEST_SIZE+3).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());
        let tree = super::construct_min_search_tree(data.as_mut());
        let mut tree_view = super::ArrayTreeView::new(data.as_ref(), &tree);

        let lnn = (0..data.len()).map(|idx| {
            super::get_left_opt(&mut tree_view, idx, idx)
        }).collect::<Vec<isize>>().into_boxed_slice();
        let rnn = (0..data.len()).map(|idx| {
            super::get_right_opt(&mut tree_view, idx, idx)
        }).collect::<Vec<isize>>().into_boxed_slice();

        validate_ansv(data.as_ref(), lnn.as_ref(), rnn.as_ref());

    }
    #[test]
    fn ansv_test() {
        let mut data = (0usize..utils::DEFAULT_TEST_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());

        let (lnn, rnn) = super::compute_ansv(data.as_mut());

        validate_ansv(data.as_ref(), lnn.as_ref(), rnn.as_ref());
    }
    #[test]
    fn ansv_linear_test() {
        let mut data = (0usize..utils::DEFAULT_TEST_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());

        let mut lnn = vec![0isize; data.len()].into_boxed_slice();
        let mut rnn = vec![0isize; data.len()].into_boxed_slice();


        super::compute_ansv_linear(data.as_ref(), lnn.as_mut(), rnn.as_mut(), 0);

        validate_ansv(data.as_ref(), lnn.as_ref(), rnn.as_ref());
    }
    #[test]
    fn ansv_linear_on_parallel_chunks_test() {
        let mut data = (0usize..utils::DEFAULT_TEST_SIZE).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(data.as_mut());

        let mut lnn = vec![0isize; data.len()].into_boxed_slice();
        let mut rnn = vec![0isize; data.len()].into_boxed_slice();

        let chunk_size = utils::rayon_chunk_size(data.len());
        generic_izip!(data.par_chunks(chunk_size), lnn.par_chunks_mut(chunk_size), rnn.par_chunks_mut(chunk_size)).enumerate()
            .for_each(|(cid, (data_chunk, lnn_chunk, rnn_chunk))| {
                super::compute_ansv_linear(data_chunk, lnn_chunk, rnn_chunk,chunk_size * cid);
            });
        generic_izip!(data.chunks(chunk_size), lnn.chunks(chunk_size), rnn.chunks(chunk_size)).enumerate()
            .for_each(|(cid, (data_chunk, lnn_chunk, rnn_chunk))| {
                let lnn_fixed = lnn_chunk.iter().map(|&val| {
                    if val == -1 {
                        val
                    } else {
                        val - (cid * chunk_size) as isize
                    }
                }).collect::<Vec<isize>>().into_boxed_slice();
                let rnn_fixed = rnn_chunk.iter().map(|&val| {
                    if val == -1 {
                        val
                    } else {
                        val - (cid * chunk_size) as isize
                    }
                }).collect::<Vec<isize>>().into_boxed_slice();
                validate_ansv(data_chunk, lnn_fixed.as_ref(), rnn_fixed.as_ref());
            });
    }
}

