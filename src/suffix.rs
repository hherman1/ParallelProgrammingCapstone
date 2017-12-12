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

enum CharType {A, B}
//const B_SIZE: usize = A_SIZE * A_SIZE;


fn gen_a_b_counts<'a>(data: &'a [u8]) -> (Box<[usize]>, Box<[[usize; B_DIM]]>) {
    let mut bounds = utils::bounds_for_num_chunks(data.len(), rayon::current_num_threads());
    adjust_bounds_to_as(data, bounds.as_mut());

    let sub_slices = utils::multi_split_slice(data, bounds.as_ref());
    let (a_buckets, b_buckets) = sub_slices.par_iter().map(|&slice: &&[u8]| {
        let mut a_buckets = [0usize; A_SIZE];
        let mut b_buckets = vec![[0usize; B_DIM]; B_DIM];

        slice.iter().rev().fold((CharType::B, 0u8), |(next_type, next_val), &cur_val: &u8| {
            if cur_val > next_val {
                a_buckets[cur_val as usize] += 1;
                return (CharType::A, cur_val)
            }
            else if cur_val < next_val {
                match next_type {
                    CharType::A => {
                        // B* case
                        b_buckets[cur_val as usize][next_val as usize] += 1;
                    },
                    CharType::B => {
                        b_buckets[next_val as usize][cur_val as usize] += 1;
                    }
                }
                return (CharType::B, cur_val)
            } else {
                match next_type {
                    CharType::A => a_buckets[cur_val as usize] += 1,
                    CharType::B => b_buckets[next_val as usize][cur_val as usize] += 1
                }
                return (next_type, cur_val);
            }
        });
        (a_buckets, b_buckets)
    }).reduce(|| ([0usize; A_SIZE], vec![[0usize; B_DIM]; B_DIM]), |(mut a_buckets_left, mut b_buckets_left), (a_buckets_right, b_buckets_right)| {
        a_buckets_left.iter_mut().zip(a_buckets_right.iter()).for_each(|(l, &r): (&mut usize, &usize)| *l += r);
        b_buckets_left.iter_mut().zip(b_buckets_right.iter()).for_each(|(sb_l, sb_r)| {
            sb_l.iter_mut().zip(sb_r.iter()).for_each(|(l, r)| *l += r);
        });
        (a_buckets_left, b_buckets_left)
    });
    (Box::new(a_buckets), b_buckets.into_boxed_slice())
}

pub fn suffix_array(data: & [u8], suffix_array: &mut [usize]) {
    let (a_buckets, b_buckets) = gen_a_b_counts(data);
    println!("{:?}", a_buckets);

}

//#[derive(Debug, Copy, Clone)]
//enum CharType {
//    S, L, LMS
//}

//fn suffix_array_2(data: & [u8], suffix_array: &mut [usize]) {
//    let mut types = vec![CharType::S; data.len()].into_boxed_slice();
//    data.iter().rev().zip(types.iter_mut().rev())
//        .fold((0u8, CharType::S), |(next_item, next_type), (&cur_item, cur_type )| {
//           if cur_item == next_item {
//               *cur_type = next_type;
//           } else if cur_item < next_item {
//               *cur_type = match next_type {
//                   CharType::LMS => CharType::S,
//                   CharType::S => CharType::S,
//                   CharType::L => CharType::LMS
//               }
//           } else {
//               *cur_type = match next_type {
//                   CharType::LMS => CharType::S,
//                   CharType::S => CharType::S,
//                   CharType::L => CharType::LMS
//               }
//           }
//           (cur_item, *cur_type)
//        });
//
//
//}

#[cfg(test)]
mod test {
    use test;
    use rayon;
    use rayon::prelude::*;
    use utils::*;


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
    fn a_b_counts_bench(bench: &mut test::Bencher) {
        let data = random_slice(BENCH_SIZE);
        bench.iter(|| super::gen_a_b_counts(data.as_ref()));
    }

    #[test]
    fn a_b_counts_test() {
        let data = random_slice(DEFAULT_TEST_SIZE);
        let (a_buckets, b_buckets) = super::gen_a_b_counts(data.as_ref());
        let sums =(0..super::A_SIZE).into_par_iter().map(|idx| {
            let mut out = a_buckets[idx];
            out += b_buckets[idx][idx..super::A_SIZE].iter().sum::<usize>();
            out += b_buckets[idx+1..super::A_SIZE].iter().map(|s| s[idx]).sum::<usize>();
            out
        }).collect::<Vec<usize>>();
        let hist = data.par_iter().fold(|| [0usize; 256], |mut hist, &byte| {
            hist[byte as usize] += 1;
            hist
        }) .reduce(|| [0usize; 256], |mut left, right| {
            left.iter_mut().zip(right.iter()).for_each(|(l, r)| *l += r);
            left
        });
        assert_eq!(sums.iter().sum::<usize>(), data.len());
        assert_eq!(sums, &hist[..]);
    }
    #[test]
    fn suffix_array_test() {
        let x = random_slice_with_zeroes(BENCH_SIZE);
        let mut suffix_array = vec![0; BENCH_SIZE];
        super::suffix_array(x.as_ref(), suffix_array.as_mut());

    }
}