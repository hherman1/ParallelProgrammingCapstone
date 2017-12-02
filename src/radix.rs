
use rayon::prelude::*;
use std;

const N_BUCKETS: usize = 2 * 2 * 2 * 2;
const STEPS_PER_CHAR: usize = 8 / 4;
const N_STEPS: usize = STEPS_PER_CHAR * 3;


fn radix_step<'a>(source: & [[u8; 3]],
                  original_indices: &'a mut [usize], original_indices_swap: &'a mut [usize],
                  current_step_num: usize,
                  counts: &mut [usize], counts_before: &mut [usize],
) {
    const PARALLEL_BATCH_COUNT: usize = 1000;
    const BATCH_SIZE: usize = 4096;

    let elements_received = original_indices.len();

    let batches =  (elements_received as f64/BATCH_SIZE as f64).ceil() as usize;

    let even_phase = 1-(current_step_num % 2);
    let sub_index = current_step_num/STEPS_PER_CHAR;

    macro_rules! get_bucket {
        ($b:expr) => {{
            let byte = $b[sub_index];
            (((byte >> even_phase*4) & 0b00001111) as usize)
        }}
    }
    if true {//batches <= PARALLEL_BATCH_COUNT {
        counts.iter_mut().for_each(|v| {
            *v = 0;
        });
        for i in 0..elements_received {
            counts[get_bucket!( source[original_indices[i]] )] += 1;
        }

    } else {
//        decision remains to be made if manually batching is faster in general than rayon's built in fold batching
//        from benchmarks it seems to be nearly the same speed .. if we don't gain much off manually batching
//        using rayon's is better.. it makes the code simpler and may get faster over time with improvements to
//        rayon.. so don't delete this code yet!

        counts.copy_from_slice(&((0..batches).into_par_iter()
            .map(|batch| {
                let mut sub_counts = [0usize; N_BUCKETS];
                for i in batch*BATCH_SIZE..std::cmp::min((batch+1)*BATCH_SIZE, elements_received) {
                    sub_counts[get_bucket!(source[original_indices[i]])] += 1;
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


    for i in 0..original_indices.len() {
        let b = source[original_indices[i]];
        let bucket_pos = get_bucket!(b);
        original_indices_swap[counts_before[bucket_pos]] = original_indices[i];
        counts_before[bucket_pos] += 1;
    }
    original_indices.copy_from_slice(original_indices_swap);

}

fn radix_recursive_manager_step<'a>(source: & [[u8; 3]],
                                    original_indices: &'a mut [usize], original_indices_swap: &'a mut [usize],
                                    current_step_num: usize,
                                    counts: &mut [usize], counts_before: &mut [usize]
) {

    const PARALLEL_ELEMENT_COUNT: usize = 128;

    radix_step(source, original_indices, original_indices_swap, current_step_num, counts, counts_before);
    if current_step_num == N_STEPS - 1 {
        return;
    }

    let elements_received: usize = original_indices.len();

    if elements_received <= 1 {
        return;
    }

    let mut sub_slices = Vec::<(&mut [usize], &mut [usize])>::with_capacity(16);

    let mut remaining_indices = original_indices;
    let mut remaining_indices_swap = original_indices_swap;

    let mut last_split_index = 0usize;
    for last_filled_index in counts_before.iter() {
        if *last_filled_index != last_split_index {

            let old_remaining_indices = remaining_indices;
            let old_remaining_indices_swap = remaining_indices_swap;

            let (original_indices_tmp, new_original_indices) = old_remaining_indices.split_at_mut(last_filled_index - last_split_index);
            let (original_indices_swap_tmp, new_original_indices_swap) = old_remaining_indices_swap.split_at_mut(last_filled_index - last_split_index);

            remaining_indices = new_original_indices;
            remaining_indices_swap = new_original_indices_swap;

            sub_slices.push((original_indices_tmp, original_indices_swap_tmp));

            last_split_index = *last_filled_index;

        }
    }

    if elements_received <= PARALLEL_ELEMENT_COUNT {
        sub_slices.iter_mut()
            .for_each(|&mut (ref mut og_indices, ref mut og_indices_swap)| {
                radix_recursive_manager_step(source,
                                             *og_indices, *og_indices_swap,
                                             current_step_num+1,
                                             counts, counts_before);
            });

    } else {
        sub_slices.par_iter_mut()
            .for_each(|&mut (ref mut og_indices, ref mut og_indices_swap)| {
                radix_recursive_manager_step(source,
                                             *og_indices, *og_indices_swap,
                                             current_step_num+1,
                                             &mut [0; N_BUCKETS], &mut [0; N_BUCKETS]);
            });
    }
}
pub fn par_radix_sort(strs: & [[u8; 3]]) -> Box<[usize]> {

//    let original_indices: &[usize] = (0..strs.len()).as_slice();
    let mut original_indices = Vec::<usize>::with_capacity(strs.len());
    original_indices.extend(0..strs.len());
    let mut original_indices = original_indices.into_boxed_slice().to_owned();

    let mut original_indices_swap = Vec::<usize>::with_capacity(strs.len());
    original_indices_swap.extend(0..strs.len());
    let mut original_indices_swap = original_indices_swap.into_boxed_slice().to_owned();

    radix_recursive_manager_step(strs, &mut original_indices, &mut original_indices_swap, 0,
                                 &mut [0; 16], &mut [0; 16]);

    return original_indices;
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
        let triplet_slice_radix = triplet_slice_radix.into_boxed_slice();

        triplet_slice.sort();

        let triplet_slice_radix_sorted: Vec<[u8; 3]> =  super::par_radix_sort(&*triplet_slice_radix).iter()
            .map(|el| {
                triplet_slice_radix[*el]
            }).collect();
        triplet_slice_radix_sorted.into_boxed_slice() == triplet_slice

    }

    fn random_triplet_slice(len: usize) -> Box<[[u8; 3]]> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; len*3];
        rng.fill_bytes(data.as_mut_slice());
        triplet_slice(data)
    }

    const BENCH_SIZE: usize = 65536 * 2;

    #[bench]
    fn radix_bench(bench: &mut test::Bencher) {
        bench.iter(|| {
            let arr = random_triplet_slice(BENCH_SIZE);
            super::par_radix_sort(&arr[..]);
        })
    }

    #[bench]
    fn radix_step_bench(bench: &mut test::Bencher) {
        let strs = random_triplet_slice(65536);

        let mut original_indices = Vec::<usize>::with_capacity(strs.len());
        original_indices.extend(0..strs.len());
        let mut original_indices = original_indices.into_boxed_slice().to_owned();

        let mut original_indices_swap = Vec::<usize>::with_capacity(strs.len());
        original_indices_swap.extend(0..strs.len());
        let mut original_indices_swap = original_indices_swap.into_boxed_slice().to_owned();


        bench.iter(|| {
            super::radix_step(&strs[..], &mut original_indices, &mut original_indices_swap, 0,
                                                &mut [0; 16], &mut [0; 16]);
        })
    }
    #[bench]
    fn sort_bench(bench: &mut test::Bencher) {

        bench.iter(|| {
            let mut arr = random_triplet_slice(BENCH_SIZE);
            arr.par_sort();
        })
    }




    #[test]
    fn simple_sort() {
        let x = [[1,3,55], [249, 24, 4], [1, 2, 127], [1,2, 126]];
        let res = vec!(3usize,2,0,1).into_boxed_slice();
        assert_eq!(super::par_radix_sort(&x[0..4] ), res );
    }
}