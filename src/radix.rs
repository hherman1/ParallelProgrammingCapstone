
use rayon::prelude::*;
use std;

// The purpose of this is to allow things which are constant for all sorted members
// in calculating their bucket to be precomputed..
// the idea was to precompute which sub_byte should be considered for [u8; 3]s
// but reading that data from a struct *slowed things down*. Very frustrating!!
// There appears to be some cost for making this all abstract too.. also frustrating!!

pub trait RadixPrecompute {
    type StepConsts: Sync = ();
    fn get_step_consts(cur_steps: usize) -> Self::StepConsts;
}

pub trait Radix where Self: RadixPrecompute {
    const N_BUCKETS: usize;
    const N_STEPS: usize;

    // The step consts must be read only because this function is intended to be run
    // highly in parallel. So we cannot achieve a mutable borrow because you can only have
    // one of those at once, whereas we can have as many read only borrows as we want
    // at a given time. So this must be a plain reference, not &mut, although
    // it would be nice if there is ever a way to make this &mut.
    fn get_bucket(self: &Self, cur_step: usize, step_consts: &Self::StepConsts) -> usize;


    const HISTOGRAM_BATCH_SIZE: usize = 4096 * 32;
    const HISTOGRAM_PARALLEL_BATCH_COUNT: usize = 4;

    const RADIX_PARALLEL_EL_COUNT: usize = 128;
}

mod radix_byte_triple {
    impl super::RadixPrecompute for [u8; 3] {
        type StepConsts = ();
        fn get_step_consts(_cur_step: usize) -> () {
            ()
        }
    }

    impl super::Radix for [u8; 3] {

        const N_BUCKETS: usize = 16;
        const N_STEPS: usize = 2 * 3;

        #[inline(always)]
        fn get_bucket(self: &[u8; 3], cur_step: usize, _step_consts: &()) -> usize {
            let even_phase = 1-(cur_step % 2) as u8;
            let sub_index = cur_step/2;
            let byte = self[sub_index];
            (((byte >> even_phase * 4) & 0b00001111) as usize)
        }
    }
}

fn radix_indices_sort_step<'a, T, F>(data: &'a [T], indices: &'a mut [usize], indices_swap: &'a mut [usize],
    current_step_num: usize,
    counts: &mut [usize], counts_before: &mut [usize],
    get_bucket: &F)
    where T: 'a,
    F: Fn(&'a [T], usize, usize) -> usize {

    let elements_received = indices.len();

    counts.iter_mut().for_each(|v| {
        *v = 0;
    });
    for i in 0..elements_received {
        counts[get_bucket(data, indices[i], current_step_num)] += 1;
    }

    counts_before[0] = 0;
    for i in 1..counts_before.len() {
        counts_before[i] = counts_before[i-1] + counts[i-1];
    }

    for i in 0..elements_received {
        let bucket_pos = get_bucket(data, indices[i], current_step_num);
        indices_swap[counts_before[bucket_pos]] = indices[i];
        counts_before[bucket_pos] += 1;
    }
}
fn radix_indices_sort_recursive_manager_step<T, F>(data: & [T],
                                                    indices: & mut [usize], indices_swap: & mut [usize],
                                                    current_step_num: usize,
                                                    counts: &mut [usize], counts_before: &mut [usize],
                                                    n_buckets: usize, n_steps: usize, get_bucket: &F)
    where T: Sync,
          F: Fn(& [T], usize, usize) -> usize + Sync + Send
{

    const PARALLEL_ELEMENT_COUNT: usize = 128;

    radix_indices_sort_step(data, indices, indices_swap, current_step_num, counts, counts_before, get_bucket);

    if current_step_num == n_steps - 1 {
        return;
    }

    let elements_received: usize = indices.len();

    if elements_received <= 1 {
        return;
    }

    let mut sub_slices = Vec::<(&mut [usize], &mut [usize])>::with_capacity(n_buckets);

    let mut remaining_indices = indices;
    let mut remaining_indices_swap = indices_swap;

    let mut last_split_index = 0usize;
    for last_filled_index in counts_before.iter() {
        if *last_filled_index != last_split_index {

            let old_remaining_indices = remaining_indices;
            let old_remaining_indices_swap = remaining_indices_swap;

            let (indices_tmp, new_indices) = old_remaining_indices.split_at_mut(last_filled_index - last_split_index);
            let (indices_swap_tmp, new_indices_swap) = old_remaining_indices_swap.split_at_mut(last_filled_index - last_split_index);

            remaining_indices = new_indices;
            remaining_indices_swap = new_indices_swap;

            sub_slices.push((indices_tmp, indices_swap_tmp));

            last_split_index = *last_filled_index;

        }
    }

    if elements_received <= PARALLEL_ELEMENT_COUNT {
        sub_slices.iter_mut()
            .for_each(|&mut (ref mut sub_indices, ref mut sub_indices_swap)| {
                radix_indices_sort_recursive_manager_step(data, *sub_indices_swap, *sub_indices,
                                                          current_step_num+1,
                                                          counts, counts_before,
                                                          n_buckets, n_steps, get_bucket);
            });

    } else {
        sub_slices.par_iter_mut()
            .for_each(|&mut (ref mut sub_indices, ref mut sub_indices_swap)| {
                radix_indices_sort_recursive_manager_step(data, *sub_indices_swap, *sub_indices,
                                                          current_step_num+1,
                                                          vec![0; n_buckets].as_mut_slice(), vec![0; n_buckets].as_mut_slice(),
                                                          n_buckets, n_steps, get_bucket);
            });
    }
}
pub fn par_radix_indices_sort<T, F>(data: &[T], indices: & mut [usize], n_buckets: usize, n_steps: usize, get_bucket: &F)
    where T: Sync,
          F: Fn(&[T], usize, usize) -> usize + Sync + Send
{
    let mut indices_swap = vec![0; indices.len()];

    radix_indices_sort_recursive_manager_step(data,indices, indices_swap.as_mut_slice(),
                                              0,
                                              vec![0; n_buckets].as_mut_slice(),vec![0; n_buckets].as_mut_slice(),
                                              n_buckets, n_steps, get_bucket);
    if n_steps % 2 == 1 {
        indices.copy_from_slice(indices_swap.as_mut_slice());
    }
}

pub fn par_radix_triplet_indices_sort(data: &[u8], indices: &mut [usize]) {
    par_radix_indices_sort(data, indices, 16, 6, &|data , index, current_step| {
        let even_phase = 1-(current_step % 2) as u8;
        let sub_index = current_step/2;
        let byte = data[index + sub_index];
        (((byte >> even_phase * 4) & 0b00001111) as usize)
    })
}

fn radix_step<'a, T, G>(data: &'a mut [T], data_swap: &'a mut [T],
                  carry: &'a mut [G], carry_swap: &'a mut [G],
                  current_step_num: usize,
                  counts: &mut [usize], counts_before: &mut [usize])
where T:'a + self::Radix + Copy + Sync + std::fmt::Debug,
    G: 'a + Copy + Sync
{

    let batch_size: usize = T::HISTOGRAM_BATCH_SIZE;
//    let parallel_batch_count: usize = T::HISTOGRAM_PARALLEL_BATCH_COUNT;

    let step_consts = T::get_step_consts(current_step_num);

    let elements_received = data.len();

    let batches =  (elements_received as f64/ batch_size as f64).ceil() as usize;


    // Writing anything but `true` here (such as the commented condition following it)
   if true { //batches <= parallel_batch_count {
        counts.iter_mut().for_each(|v| {
            *v = 0;
        });
        for i in 0..elements_received {
            counts[data[i].get_bucket(current_step_num, &step_consts)] += 1;
        }
    } else {

//        decision remains to be made if manually batching is faster in general than rayon's built in fold batching
//        from benchmarks it seems to be nearly the same speed .. if we don't gain much off manually batching
//        using rayon's is better.. it makes the code simpler and may get faster over time with improvements to
//        rayon.. so don't delete this code yet!

        counts.copy_from_slice(&((0..batches).into_par_iter()
            .map(|batch| {
                let mut sub_counts = vec![0usize; <T as Radix>::N_BUCKETS];
                for i in batch* batch_size..std::cmp::min((batch+1)* batch_size, elements_received) {
                    sub_counts[data[i].get_bucket(current_step_num, &step_consts)] += 1;
                }
                sub_counts
            })
            .reduce(|| vec![0usize; T::N_BUCKETS], |mut a, b| {
                for i in 0..T::N_BUCKETS {
                    a[i] += b[i];
                }
                a
            })).as_slice() as &[usize]);

    }

    counts_before[0] = 0;
    for i in 1..counts_before.len() {
        counts_before[i] = counts_before[i-1] + counts[i-1];
    }

    for i in 0..elements_received {
        let bucket_pos = data[i].get_bucket(current_step_num, &step_consts);
        data_swap[counts_before[bucket_pos]] = data[i];
        carry_swap[counts_before[bucket_pos]] = carry[i];
        counts_before[bucket_pos] += 1;
    }

}


fn radix_recursive_manager_step<'a, T, G>(data: &'a mut [T], data_swap: &'a mut [T],
                                    carry: &'a mut [G], carry_swap: &'a mut [G],
                                    current_step_num: usize,
                                    counts: &mut [usize], counts_before: &mut [usize])
    where T: 'a + Radix + Copy + Sync + Send + std::fmt::Debug,
        G: 'a + Copy + Sync + Send
{

    const PARALLEL_ELEMENT_COUNT: usize = 128;

    radix_step(data, data_swap, carry, carry_swap,current_step_num, counts, counts_before);

    if current_step_num == T::N_STEPS - 1 {
        return;
    }

    let elements_received: usize = data.len();

    if elements_received <= 1 {
        return;
    }

    let mut sub_slices = Vec::<(&mut [T], &mut [T], &mut [G], &mut [G])>::with_capacity(T::N_BUCKETS);

    let mut remaining_data = data;
    let mut remaining_data_swap = data_swap;

    let mut remaining_carry = carry;
    let mut remaining_carry_swap = carry_swap;

    let mut last_split_index = 0usize;
    for last_filled_index in counts_before.iter() {
        if *last_filled_index != last_split_index {

            let old_remaining_data = remaining_data;
            let old_remaining_data_swap = remaining_data_swap;

            let old_remaining_carry = remaining_carry;
            let old_remaining_carry_swap = remaining_carry_swap;

            let (data_tmp, new_data) = old_remaining_data.split_at_mut(last_filled_index - last_split_index);
            let (data_swap_tmp, new_data_swap) = old_remaining_data_swap.split_at_mut(last_filled_index - last_split_index);

            let (carry_tmp, new_carry) = old_remaining_carry.split_at_mut(last_filled_index - last_split_index);
            let (carry_swap_tmp, new_carry_swap) = old_remaining_carry_swap.split_at_mut(last_filled_index - last_split_index);

            remaining_data = new_data;
            remaining_data_swap = new_data_swap;

            remaining_carry = new_carry;
            remaining_carry_swap = new_carry_swap;

            sub_slices.push((data_tmp, data_swap_tmp, carry_tmp, carry_swap_tmp));

            last_split_index = *last_filled_index;

        }
    }

    if elements_received <= PARALLEL_ELEMENT_COUNT {
        sub_slices.iter_mut()
            .for_each(|&mut (ref mut sub_data, ref mut sub_data_swap, ref mut sub_carry, ref mut sub_carry_swap)| {
                radix_recursive_manager_step(*sub_data_swap, *sub_data,
                                             *sub_carry_swap, *sub_carry,
                                             current_step_num+1,
                                             counts, counts_before);
            });

    } else {
        sub_slices.par_iter_mut()
            .for_each(|&mut (ref mut sub_data, ref mut sub_data_swap, ref mut sub_carry, ref mut sub_carry_swap)| {
                radix_recursive_manager_step(*sub_data_swap, *sub_data,
                                             *sub_carry_swap, *sub_carry,
                                             current_step_num+1,
                                             vec![0; T::N_BUCKETS].as_mut_slice(),vec![0; T::N_BUCKETS].as_mut_slice());
            });
    }
}

pub fn par_radix_sort<'a, T, G>(data: &'a mut [T], carry: &'a mut [G])
    where T:'a + Radix + Copy + Sync + Send + Clone + Default + std::fmt::Debug,
    G: 'a + Copy + Sync + Send
{
    let mut data_swap = vec![<T>::default(); data.len()];
    let mut carry_swap = carry.to_vec();

    radix_recursive_manager_step(data, data_swap.as_mut_slice(),
                                 carry, carry_swap.as_mut_slice(),
                                 0,
                                 vec![0; T::N_BUCKETS].as_mut_slice(), vec![0; T::N_BUCKETS].as_mut_slice());
    if T::N_STEPS % 2 == 1 {
        data.copy_from_slice(data_swap.as_mut_slice());
    }
}
#[cfg(test)]
mod test {
    use test;
    use rayon::prelude::ParallelSliceMut;

    use utils::*;



    fn matches_sort(triplet_slice: &mut [[u8; 3]]) -> bool {
        let mut triplet_slice_radix = vec![[0, 0, 0]; triplet_slice.len()];
        triplet_slice_radix.copy_from_slice(&triplet_slice[..]);

        let mut order: Vec<usize> = (0..triplet_slice.len()).collect();

        triplet_slice.sort();

        super::par_radix_sort(triplet_slice_radix.as_mut_slice(), order.as_mut_slice());
        triplet_slice_radix.as_slice() == &*triplet_slice
    }


    quickcheck! {
        fn matches_sort_quickcheck(data: Vec<u8>) -> bool {
            matches_sort(triplet_slice(data).as_mut())
        }
    }
    const TEST_TRIPLET_NUM: usize = 20;

    #[test]
    fn matches_sort_order_test() {
        let mut arr = random_triplet_slice(TEST_TRIPLET_NUM);
        let mut order: Vec<usize> = (0..arr.len()).collect();

        let mut arr_clone: Vec<(usize, [u8; 3])> = arr.clone().into_vec().iter().enumerate()
            .map(|(idx, v)| (idx, *v)).collect();
        arr_clone.sort_by_key(|el| el.1);
        let x: Vec<usize> = arr_clone.iter().map(|el| el.0).collect();
        super::par_radix_sort(&mut *arr, order.as_mut_slice());

        assert_eq!(x, order);
    }

    #[test]
    fn matches_indices_sort_order_test() {
        let dat = random_slice_with_zeroes(TEST_TRIPLET_NUM);
        let dat_triplet = to_suffix_triplet_slice(dat.as_ref());

        let mut indices = (0..TEST_TRIPLET_NUM - 2).collect::<Vec<usize>>();


        let mut arr_clone: Vec<(usize, [u8; 3])> = indices.iter().map(|i| (*i, dat_triplet[*i])).collect();
        arr_clone.sort_by_key(|el| el.1);
        let sorted_order: Vec<usize> = arr_clone.iter().map(|el| el.0).collect();
        super::par_radix_triplet_indices_sort(dat.as_ref(), indices.as_mut());

        assert_eq!(sorted_order, indices);
    }

    #[test]
    fn matches_sort_test() {
        assert!(matches_sort(random_triplet_slice(BENCH_SIZE).as_mut()));
    }

    #[test]
    fn matches_sort_fixed() {
        let dat = vec![0, 0, 27];
        assert!(matches_sort(triplet_slice(dat).as_mut()))
    }

    #[bench]
    fn radix_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            let mut order: Vec<usize> = (0..arr.len()).collect();
            super::par_radix_sort(&mut *arr, order.as_mut_slice());
        })
    }

    #[test]
    fn radix_bench_test() {
        let mut arr = random_triplet_slice(BENCH_SIZE);
        let mut order: Vec<usize> = (0..arr.len()).collect();
        super::par_radix_sort(&mut *arr, order.as_mut_slice());
    }

    #[bench]
    fn radix_indices_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let arr = random_slice_with_zeroes(BENCH_SIZE);
            let mut order: Vec<usize> = (0..arr.len() - 2).collect();
            super::par_radix_triplet_indices_sort(arr.as_ref(), order.as_mut());
        })
    }

    #[bench]
    fn radix_step_bench(bench: &mut test::Bencher) {
        let mut data = random_triplet_slice(BENCH_SIZE);
        let mut data_swap = vec![[0, 0, 0]; BENCH_SIZE];

        let mut carry: Vec<usize> = (0..data.len()).collect();
        let mut carry_swap = carry.clone();

        bench.iter(|| {
            super::radix_step(&mut *data, data_swap.as_mut_slice(),
                              carry.as_mut_slice(), carry_swap.as_mut_slice(),
                              0,
                              &mut [0; 16], &mut [0; 16]);
        })
    }

    #[bench]
    fn par_sort_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            arr.par_sort_unstable();
        })
    }

    #[bench]
    fn sort_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            arr.sort();
        })
    }

    #[test]
    fn simple_sort() {
        let mut x = [[1, 3, 55], [249, 24, 4], [1, 2, 127], [1, 2, 126]];
        let mut carry: Vec<usize> = (0..4).collect();
        let sorted = [[1, 2, 126], [1, 2, 127], [1, 3, 55], [249, 24, 4]];
        super::par_radix_sort(&mut x[..], carry.as_mut_slice());
        assert_eq!(x, sorted);
    }
}

