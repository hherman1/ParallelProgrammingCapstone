use radix;
use rayon::prelude::*;
use rayon;
use utils;

// Data must end with two 0 entries which are not used
//fn suffix_array(data: & [u8], suffix_array: &mut [usize]) {
//
//    let mut mod_3_indices = (0usize..data.len()-2).filter(|v| v%3 != 0).collect::<Vec<usize>>();
//    radix::par_radix_triplet_indices_sort(data, mod_3_indices.as_mut());
//
//
//    let uniques: usize = mod_3_indices.par_iter()
//        .zip(mod_3_indices.par_iter().skip(1))
//        .map(|(&i, &j)| {
//            (data[i] != data[j] ||
//                data[i+1] != data[j+1] ||
//                data[i+2] != data[j+2])
//                as usize
//        })
//        .sum();
////    let uniques: Vec<usize> = mod_3_indices.par_iter()
////        .zip(mod_3_indices.par_iter().skip(1))
////        .filter(|(&i, &j)| {
////            data[i] != data[j] ||
////                data[i+1] != data[j+1] ||
////                data[i+2] != data[j+2])
////        })
////        .map(|(&i, &j))
//
////    let mut partitioned_indices = vec!
//    mod_3_indices.par_iter().for_each(|&i| {
//
//    });
//
//    if uniques < mod_3_indices.len() {
//
//    }
//
////    (0..mod_3_indices.len()).par_iter()
////        .map(|i)
//}


fn adjust_bounds_to_as(data: &[u8], bounds: &mut [usize]) {
    bounds.par_iter_mut().for_each(|bound| {
        if *bound == data.len() {
            return;
        }
        while *bound > 0 && data[*bound - 1] < data[*bound] {
            *bound -= 1;
        }
    });
}

const A_SIZE: usize = 256;
const B_DIM: usize = A_SIZE;

enum CharType {A, B, BSTAR}
//const B_SIZE: usize = A_SIZE * A_SIZE;

// This is explained in Nong 2009 and Labeit 2017.
#[inline]
fn cur_type(next_type:CharType, next_val: u8, cur_val: u8) -> CharType {
    if cur_val > next_val {
        return CharType::A
    } else if cur_val < next_val {
        match next_type {
            CharType::A => return CharType::BSTAR,
            _ => return CharType::B
        }
    } else {
        return match next_type {
            CharType::BSTAR => CharType::B,
            _ => next_type
        }
    }

}

// This is explained NOWHERE. WTF IS THIS.
// Guess: calculating the ranges for each char type in the final sorted suffix array
// EG: character 0 gets positions 0 through 15, which includes its type A suffixes and
// its type B and BSTAR suffixes which begin with 0.
// This is called in gen_ab_counts_and_b_star_indices.

// After inspection, I can confirm that this calculates prefix sums accross the buckets in lexicographic order.
// After running this bucket_b in the bstar positions contains a prefix sum of the b_star counts
// Bucket_a seemingly contains the ranges for suffixes starting with a given char in the final sorted array.
// Bucket_b seems worthless.
fn calculate_bucket_offsets(bucket_a: &mut [usize], bucket_b: &mut [[usize; B_DIM]]) {
    let mut a_b_prefix_sum = 0usize;
    let mut bstar_prefix_sum = 0usize;

    (0..A_SIZE).for_each(|char0| {
        let cur_a_bucket_val = bucket_a[char0];
        bucket_a[char0] = a_b_prefix_sum + bstar_prefix_sum; // As they say in the original... "Start point"
        a_b_prefix_sum += cur_a_bucket_val + bucket_b[char0][char0];

        (char0 + 1..A_SIZE).for_each(|char1| {
            // Prefix sum in lexicographic order over BSTAR buckets for char0.
            bstar_prefix_sum += bucket_b[char0][char1];
            bucket_b[char0][char1] = bstar_prefix_sum; // As they say in the original code.. "End point"

            a_b_prefix_sum += bucket_b[char1][char0];

        })
    })
}

// Counts occurences of A, B, and BSTAR bytes and byte pairs
// finds indexes of all BSTAR bytes
fn gen_a_b_offsets_and_b_star_indices<'a>(data: &'a [u8]) -> (Box<[usize]>, Box<[[usize; B_DIM]]>, Box<[usize]>) {
    let mut bounds = utils::bounds_for_num_chunks(data.len(), rayon::current_num_threads());
    adjust_bounds_to_as(data, bounds.as_mut());

    let sub_slices = utils::multi_split_slice(data, bounds.as_ref());
    let mut nums_b_star_indices = vec![0usize; bounds.len()].into_boxed_slice();

    let (mut a_buckets, mut b_buckets) = sub_slices.par_iter().zip(nums_b_star_indices.par_iter_mut())
        .map(|(&slice, num_b_star_indices): (&&[u8], &mut usize)| {

        let mut a_buckets = [0usize; A_SIZE];
        let mut b_buckets = vec![[0usize; B_DIM]; B_DIM];

        slice.iter().rev().enumerate().fold((CharType::B, 0u8), |(next_type, next_val), (idx, &cur_val): (usize, &u8)| {
            let cur_type = cur_type(next_type, next_val, cur_val);
            match cur_type {
                CharType::A => a_buckets[cur_val as usize] += 1,
                CharType::B => b_buckets[next_val as usize][cur_val as usize] += 1,
                CharType::BSTAR => {
                    *num_b_star_indices += 1;
                    b_buckets[cur_val as usize][next_val as usize] += 1;
                }
            }
            return (cur_type, cur_val)
        });

        (a_buckets, b_buckets)

    }).reduce(|| ([0usize; A_SIZE], vec![[0usize; B_DIM]; B_DIM]),
              |(mut a_buckets_left, mut b_buckets_left), (a_buckets_right, b_buckets_right)| {

                  a_buckets_left.iter_mut().zip(a_buckets_right.iter()).for_each(|(l, &r): (&mut usize, &usize)| *l += r);
                  b_buckets_left.iter_mut().zip(b_buckets_right.iter()).for_each(|(sb_l, sb_r)| {
                      sb_l.iter_mut().zip(sb_r.iter()).for_each(|(l, r)| *l += r);
                  });
                  (a_buckets_left, b_buckets_left)

              });

    let (_, b_star_indices) = rayon::join(
        || calculate_bucket_offsets(&mut a_buckets, b_buckets.as_mut()),  // Not related to the second block of code here.
        || {
            // Goal here is to collect the indices of the BSTAR bytes
            let num_b_star_indices = nums_b_star_indices.iter().sum::<usize>();
            let mut b_star_indices = vec![0usize; num_b_star_indices].into_boxed_slice();
            let mut b_star_bounds = nums_b_star_indices;
            b_star_bounds.iter_mut().fold(0usize, |sum, cur| {
                *cur += sum;
                *cur
            });

            {
                let mut sub_b_star_indices = utils::multi_split_mut_slice(&mut b_star_indices, b_star_bounds.as_ref());
                sub_slices.par_iter().zip(sub_b_star_indices.par_iter_mut()).zip(bounds.par_iter())
                    .for_each(|((&slice, b_star_indices), &end_index)| {
                        let mut bsi_iterator = b_star_indices.iter_mut().rev();

                        // Iterate through the array the same way as the first time, still tracking state as we go
                        slice.iter().rev().enumerate().fold((CharType::B, 0u8),
                                                            |(next_type, next_val), (data_idx, &cur_val)| {
                                                                let cur_type = cur_type(next_type, next_val, cur_val);
                                                                match cur_type {
                                                                    CharType::BSTAR => *(bsi_iterator.next().unwrap()) = end_index - data_idx - 1,
                                                                    _ => {}
                                                                }
                                                                return (cur_type, cur_val);
                                                            });
                    });
            }

            b_star_indices
        });

    (Box::new(a_buckets), b_buckets.into_boxed_slice(), b_star_indices)
}


// God only knows the purpose of this fucking trasher
fn init_b_star(data: &[u8], b_offsets: &mut [[usize; B_DIM]], bstar_indices: &[usize]) {
    let mut scratchpads = vec![vec![[0usize; B_DIM]; B_DIM].into_boxed_slice(); rayon::current_num_threads()].into_boxed_slice();
    {
        utils::n_split_slice(bstar_indices, scratchpads.len()).par_iter().zip(scratchpads.par_iter_mut())
            .for_each( |(&sub_bstar_indices, scratchpad): (&&[usize], &mut Box<[[usize;B_DIM]]>)| {
                sub_bstar_indices.iter().for_each(|&idx| {
                    let char0 = data[idx];
                    let char1 = data[idx+1];
                    scratchpad[char0 as usize][char1 as usize] += 1;
                })
            });
    }

//    b_offsets.par_iter().enumerate().for_each(|(char0, row): (usize, &[usize; B_DIM])| {
//        row.par_iter().enumerate().for_each(|(char1, &val): (usize, &usize)| {
//            let mut sum = val;
//            unsafe {
//                let x: Box<[[usize; B_DIM]]> = scratchpads[0];
//                (0..scratchpads.len()).for_each(|idx| {
//                    sum -= scratchpads[idx][char0][char1];
//                    scratchpads[idx][char0][char1] = sum + scratchpads[idx][char0][char1];
//                })
//            };
//        });
//    });

//    dbg!(scratchpads.iter().map(|sp| sp.iter().map(|row| row.iter().sum::<usize>()).sum::<usize>()).sum::<usize>(), bstar_indices.len());
//    scratchpads.iter().for_each(|scratchpad| {
//        dbg!("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
//        scratchpad.iter().for_each(|row| dbg!(&row[..]));
//    });

}

pub fn suffix_array(data: & [u8], suffix_array: &mut [usize]) {
    let (a_buckets, b_buckets, b_star_indices) = gen_a_b_offsets_and_b_star_indices(data);
    println!("{:?}", a_buckets);

}


#[cfg(test)]
mod test {
    use test;
    use rayon;
    use rayon::prelude::*;
    use utils::*;

    #[test]
    fn init_b_star_test() {
        let data = random_slice(DEFAULT_TEST_SIZE);
        let (mut a_offsets, mut b_offsets, mut b_star_indices) = super::gen_a_b_offsets_and_b_star_indices(data.as_ref());
        super::init_b_star(data.as_ref(), b_offsets.as_mut(), b_star_indices.as_ref());
    }
    #[test]
    fn a_bounds_test() {
        let data = random_slice(DEFAULT_TEST_SIZE);
        let mut bounds = bounds_for_num_chunks(data.len(), rayon::current_num_threads());
        super::adjust_bounds_to_as(data.as_ref(), bounds.as_mut());
        bounds.iter().for_each(|&bound| {
            if bound != data.len() {
                assert!(data[bound-1] > data[(bound).min(data.len()-1)])
            }
        });
    }

    #[bench]
    fn a_b_offsets_and_b_star_indices_bench(bench: &mut test::Bencher) {
        let data = random_slice(BENCH_SIZE);
        bench.iter(|| super::gen_a_b_offsets_and_b_star_indices(data.as_ref()));
    }

    #[test]
    fn a_b_counts_and_b_star_indices_test() {
        let data = random_slice(DEFAULT_TEST_SIZE);
        let (a_offsets, b_offsets, b_star_indices) = super::gen_a_b_offsets_and_b_star_indices(data.as_ref());

        // Check things are sorted and so on as we expect
        let mut histogram_prefix_sum = data.iter().fold([0usize; super::A_SIZE], |mut hist, byte| {
            hist[*byte as usize] += 1;
            hist
        });
        histogram_prefix_sum.iter_mut().fold(0, |sum, count| {
            let new_sum = sum + *count;
            *count = sum;
            new_sum
        } );
        assert_eq!(&histogram_prefix_sum[..], a_offsets.as_ref());

        // The prefix sum of the counts of b_star indices should end as the count of the total
        // number of b_star indices.
        assert_eq!(b_star_indices.len(), b_offsets[super::A_SIZE-2][super::A_SIZE-1]);

        assert_eq!(a_offsets[0], 0);
        let mut a_offsets_clone = a_offsets.clone();
        a_offsets_clone.par_sort();
        assert_eq!(a_offsets_clone, a_offsets);

        b_offsets.par_iter().enumerate().for_each(|(idx, offsets)| {
            let bstars = &offsets[idx+1..];
            let mut bstars_clone = bstars.to_vec().into_boxed_slice();
            bstars_clone.par_sort();
            assert_eq!(bstars_clone.as_ref(), bstars);
        });

        // Check indices properties
        b_star_indices.par_iter().for_each(|&idx| {
            if !(data[idx] < data[idx + 1]) {
                dbg!(idx, data[idx-1], data[idx], data[idx+1])
            }
            assert!(data[idx] < data[idx + 1]);
        });
        let mut sorted = b_star_indices.clone();
        sorted.sort();
        assert_eq!(b_star_indices, sorted);
    }
    #[test]
    fn suffix_array_test() {
        let x = random_slice_with_zeroes(BENCH_SIZE);
        let mut suffix_array = vec![0; BENCH_SIZE];
        super::suffix_array(x.as_ref(), suffix_array.as_mut());

    }
}