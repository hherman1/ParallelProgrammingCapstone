
use rayon::prelude::*;
use std;

const N_BUCKETS: usize = 2 * 2 * 2 * 2;
const STEPS_PER_CHAR: usize = 8 / 4;
const N_STEPS: usize = STEPS_PER_CHAR * 3;

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
    fn get_bucket(self: Self, cur_step: usize, step_consts: &Self::StepConsts) -> usize;


    const HISTOGRAM_BATCH_SIZE: usize = 4096 * 32;
    const HISTOGRAM_PARALLEL_BATCH_COUNT: usize = 4;

    const RADIX_PARALLEL_EL_COUNT: usize = 128;
}

mod radix_byte_triple {
    impl super::RadixPrecompute for [u8; 3] {
        type StepConsts = ();
        fn get_step_consts(cur_step: usize) -> () {
            ()
        }
    }

    impl super::Radix for [u8; 3] {

        const N_BUCKETS: usize = 16;
        const N_STEPS: usize = 2 * 3;

        #[inline(always)]
        fn get_bucket(self: [u8; 3], cur_step: usize, step_consts: &()) -> usize {
            let even_phase = 1-(cur_step % 2) as u8;
            let sub_index = cur_step/2;
            let byte = self[sub_index];
            (((byte >> even_phase * 4) & 0b00001111) as usize)
        }
    }
}

fn radix_step<'a, T>(data: &'a mut [T], data_swap: &'a mut [T],
                  current_step_num: usize,
                  counts: &mut [usize], counts_before: &mut [usize])
where T:'a + Radix + Copy + Sync
{

    let batch_size: usize = T::HISTOGRAM_BATCH_SIZE;
    let parallel_batch_count: usize = T::HISTOGRAM_PARALLEL_BATCH_COUNT;

    let step_consts = T::get_step_consts(current_step_num);

    let elements_received = data.len();

    let batches =  (elements_received as f64/ batch_size as f64).ceil() as usize;


    // Writing anything but `true` here (such as the commented condition following it)
    // results in abuot 3 ms of slowdown when benched on 65536 * 8 random byte triplets.
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
                let mut sub_counts = [0usize; N_BUCKETS];
                for i in batch* batch_size..std::cmp::min((batch+1)* batch_size, elements_received) {
                    sub_counts[data[i].get_bucket(current_step_num, &step_consts)] += 1;
                }
                sub_counts
            })
            .reduce(|| [0usize; N_BUCKETS], |mut a, b| {
                for i in 0..N_BUCKETS {
                    a[i] += b[i];
                }
                a
            })) as &[usize]);

    }

    counts_before[0] = 0;
    for i in 1..counts_before.len() {
        counts_before[i] = counts_before[i-1] + counts[i-1];
    }

    for i in 0..elements_received {
        let bucket_pos = data[i].get_bucket(current_step_num, &step_consts);
        data_swap[counts_before[bucket_pos]] = data[i];
        counts_before[bucket_pos] += 1;
    }
    data.copy_from_slice(data_swap);

}

fn radix_recursive_manager_step<'a, T>(data: &'a mut [T], data_swap: &'a mut [T],
                                    current_step_num: usize,
                                    counts: &mut [usize], counts_before: &mut [usize])
    where T:'a + Radix + Copy + Sync + Send
{

    const PARALLEL_ELEMENT_COUNT: usize = 128;

    radix_step(data, data_swap, current_step_num, counts, counts_before);

    if current_step_num == N_STEPS - 1 {
        return;
    }

    let elements_received: usize = data.len();

    if elements_received <= 1 {
        return;
    }

    let mut sub_slices = Vec::<(&mut [T], &mut [T])>::with_capacity(N_BUCKETS);

    let mut remaining_data = data;
    let mut remaining_data_swap = data_swap;

    let mut last_split_index = 0usize;
    for last_filled_index in counts_before.iter() {
        if *last_filled_index != last_split_index {

            let old_remaining_data = remaining_data;
            let old_remaining_data_swap = remaining_data_swap;

            let (data_tmp, new_data) = old_remaining_data.split_at_mut(last_filled_index - last_split_index);
            let (data_swap_tmp, new_data_swap) = old_remaining_data_swap.split_at_mut(last_filled_index - last_split_index);

            remaining_data = new_data;
            remaining_data_swap = new_data_swap;

            sub_slices.push((data_tmp, data_swap_tmp));

            last_split_index = *last_filled_index;

        }
    }

    if elements_received <= PARALLEL_ELEMENT_COUNT {
        sub_slices.iter_mut()
            .for_each(|&mut (ref mut sub_data, ref mut sub_data_swap)| {
                radix_recursive_manager_step(*sub_data, *sub_data_swap,
                                             current_step_num+1,
                                             counts, counts_before);
            });

    } else {
        sub_slices.par_iter_mut()
            .for_each(|&mut (ref mut sub_data, ref mut sub_data_swap)| {
                radix_recursive_manager_step(*sub_data, *sub_data_swap,
                                             current_step_num+1,
                                             &mut [0; N_BUCKETS], &mut [0; N_BUCKETS]);
            });
    }
}
pub fn par_radix_sort<'a, T>(data: &'a mut [T])
    where T:'a + Radix + Copy + Sync + Send + Clone + Default
{
    let mut data_swap = vec![<T>::default(); data.len()];

    radix_recursive_manager_step(data, data_swap.as_mut_slice(), 0,
                                 &mut [0; 16], &mut [0; 16]);
}

#[cfg(test)]
mod test {
    use test;
    use rand;
    use rand::Rng;
    use rayon::prelude::ParallelSliceMut;
    quickcheck! {
        fn matches_sort_quickcheck(data: Vec<u8>) -> bool {
            matches_sort(data)
        }
    }
    #[test]
    fn matches_sort_test() {
        let mut rng = rand::thread_rng();
        let mut data = [0u8; 2048];
        rng.fill_bytes(&mut data);
        assert!(matches_sort(data.to_vec()));
    }

    #[test]
    fn matches_sort_fixed() {
        let dat = vec![0,0,27];
        assert!(matches_sort(dat))
    }

    fn triplet_slice(data: Vec<u8>) -> Box<[[u8; 3]]> {
        let mut res = Vec::<[u8; 3]>::with_capacity(data.len()/3);
        for i in 0..data.len()/3 {
            res.push([data[3*i], data[3*i + 1], data[3*i + 2]]);
        }
        res.into_boxed_slice()
    }

    fn matches_sort(data: Vec<u8>) -> bool {

        let mut triplet_slice = triplet_slice(data);

        let mut triplet_slice_radix = vec![[0,0,0]; triplet_slice.len()];
        triplet_slice_radix.copy_from_slice(&triplet_slice[..]);

        triplet_slice.sort();

        super::par_radix_sort(triplet_slice_radix.as_mut_slice());
        triplet_slice_radix.as_slice() == &*triplet_slice

    }

    fn random_triplet_slice(len: usize) -> Box<[[u8; 3]]> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; len*3];
        rng.fill_bytes(data.as_mut_slice());
        triplet_slice(data)
    }

    const BENCH_SIZE: usize = 65536 * 8;

    #[bench]
    fn radix_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            super::par_radix_sort(&mut *arr);
        })
    }

    #[bench]
    fn radix_step_bench(bench: &mut test::Bencher) {
        let mut data = random_triplet_slice(BENCH_SIZE);

        let mut data_swap = vec![[0,0,0]; BENCH_SIZE];


        bench.iter(|| {
            super::radix_step(&mut *data, data_swap.as_mut_slice(),
                              0,
                              &mut [0; 16], &mut [0; 16]);
        })
    }

    #[bench]
    fn par_sort_bench(bench: &mut test::Bencher) {

        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            arr.par_sort();
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
        let mut x = [[1,3,55], [249, 24, 4], [1, 2, 127], [1,2, 126]];
        let sorted = [[1,2,126], [1, 2, 127], [1, 3, 55], [249, 24, 4]];
        super::par_radix_sort(&mut x[..]);
        assert_eq!(x, sorted);
    }
}