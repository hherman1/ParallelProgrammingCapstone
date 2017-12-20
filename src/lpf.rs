use rayon::prelude::*;
use rayon;
use utils;
use std::convert::AsMut;
use serial_suffix;
use ansv;
use std;

// Just iterates through the string until they're not equal, and counts how long the iteration lasted.
#[inline(always)]
fn get_lcp(l: &[u8], r: &[u8]) -> usize {
    l.iter().zip(r.iter()).take_while(|&(&l_el, &r_el)| l_el == r_el).count()
}

// Explanation:
// - llcp & rlcp:
//     llcp tells us how long our common prefix is with our nearest smaller lexicographic
// neighbor (that appeared before us in the string - guarenteed by the ANSV algorithm -left_elements
// and right_elements), and rlcp tells us how long our common prefix is with our nearest larger
// lexicographic neighbor (again, that appeared before us in the string, as guarenteed by ANSV
// left_elements and right_elements).
//     Our nearest lexicographic neighbors of the suffixes that appeared before us
// are guarenteed to have the most in common with us as the suffixes that appeared before us.
//
// - rank:
//     If we are considering a suffix at position k in the original string, rank[k] is that suffixes
// position in the suffix array. In other (confusing) words: suffix_array[rank[k]] = k. We use this
// to find our left and right nearest neighbors, since lnns and rnns were calculated on the suffix_array.
//
// - left_elements & right_elements:
//     Recall that the suffix array is a collection of indices. If we're considering suffix_array[k], then
// left_elements[k] tells us the first position i where i < k in suffix_array such that suffix_array[i] < suffix_array[k].
// right_elements is the same but it tells us the first position i with i > k (to the right of k) such that
// suffix_array[i] < suffix_array[k]. In other words, since suffix_array[i] < suffix_array[k] the suffix pointed to by
// suffix_array[i] appears earlier in the string than the one pointed to by suffix_array[k]. Since suffix_array
// represents a lexicographic ordering of all the suffixes, the suffix which appears first to the left with this property
// is the suffix which is lexicographically closest and smaller than us while still appearing before us in the string.
// Likewise the suffix which appears first to the right is the suffix which is lexicographically closest and larger than us
// while still appearing before us in the string.
//     Only strings which are lexicographically closest to us are capable of being the source of the longest previous factor,
// and there are only two lexicographically closest: the closest lexicographically smaller and closest lexicographically larger
// suffixes. Then we just have to compare what our lcp is with each of these suffixes and we know immediately what our total lpf is,
// since as we just explained, it must be one of these two, so it is which ever is larger.
//
//     In the main for loop, we know that a suffix following another suffix will have at least the same LPF as the previous
// suffix - 1, since its composed of the same letters shy of the first one, so that whatever the previous suffix's LPF suffix was,
// that same suffix will produce LPF - 1 characters for us.. that may be slightly confusing.

pub fn lpf_3(data: &[u8], suffix_array: &[usize], left_elements: &[isize], right_elements: &[isize]) -> (Box<[usize]>, Box<[isize]>) {

    let ar_len = data.len();

    let mut longest_previous_factor = vec![0usize; ar_len].into_boxed_slice();
    let mut prev_occ = vec![0isize; ar_len].into_boxed_slice();


    let mut rank_array = longest_previous_factor;
    suffix_array.par_iter().enumerate().for_each(|(i, &data_i)| {
        unsafe {
            *(rank_array.as_ptr() as *mut usize).offset(data_i as isize) = i;
        }
    });

    let mut longest_previous_factor = rank_array;

    let size = utils::rayon_chunk_size(ar_len);

    longest_previous_factor.as_mut().par_chunks_mut(size).zip(prev_occ.as_mut().par_chunks_mut(size))
        .enumerate()
        .for_each(|(chunk_idx, (lpf_chunk, prev_occ_chunk))| {

            lpf_chunk.iter_mut().zip(prev_occ_chunk.iter_mut()).enumerate() .fold((0, 0),
                 |(prev_llcp, prev_rlcp), (el_idx, (lpf_chunk_el, prev_occ_chunk_el))| {

                     let abs_start_pos = chunk_idx * size + el_idx;

                     let rank = *lpf_chunk_el;

                     let left = left_elements[rank];
                     let right = right_elements[rank];

                     let mut llcp: usize = if left != -1 {
                         let min_cur_llpc_val = (prev_llcp as isize - 1).max(0) as usize;
                         min_cur_llpc_val + get_lcp(&data[suffix_array[left as usize] + min_cur_llpc_val ..], &data[abs_start_pos + min_cur_llpc_val..])
                     } else {
                         0
                     };

                     let mut rlcp: usize = if right != -1 {
                         let min_cur_rlpc_val = (prev_rlcp as isize - 1).max(0) as usize;
                         min_cur_rlpc_val + get_lcp(&data[suffix_array[right as usize] + min_cur_rlpc_val ..], &data[abs_start_pos + min_cur_rlpc_val..])
                     } else {
                         0
                     };

                     if llcp == 0 && rlcp == 0  {
                         *prev_occ_chunk_el = -1;
                         *lpf_chunk_el = 1;
                     } else if llcp > rlcp {
                         *prev_occ_chunk_el = suffix_array[left as usize] as isize;
                         *lpf_chunk_el = llcp;
                     } else {
                         *prev_occ_chunk_el = suffix_array[right as usize] as isize;
                         *lpf_chunk_el = rlcp;
                     }

                     (llcp, rlcp)
                 });
        });
    (longest_previous_factor, prev_occ)
}

#[cfg(test)]
mod lpf_testing {
    use utils;
    use rand;
    use rand::*;
    use rayon::prelude::*;
    use serial_suffix;
    use saxx;
    use ansv;
    use test;

    #[bench]
    fn lpf_calculator_bench(bencher: &mut test::Bencher) {
        let data = utils::random_slice(utils::BENCH_SIZE);
        let esa = saxx::Esaxx::<i64>::esaxx(data.as_ref()).unwrap();
        let sa = esa.sa.into_boxed_slice();
        let sa = sa.iter().map(|&v| {
            v as usize
        }).collect::<Vec<usize>>().into_boxed_slice();

        let (left_elements, right_elements) = ansv::compute_ansv(sa.as_ref());

        bencher.iter(|| {
            super::lpf_3(data.as_ref(), sa.as_ref(), left_elements.as_ref(), right_elements.as_ref());
        });

    }

    #[test]
    fn changed_this_name_so_idea_would_let_me_commit_thanks_mack_hartley_whose_social_security_number_is_123_45_6789() {
        let data = utils::random_slice(utils::DEFAULT_TEST_SIZE);
        let esa = saxx::Esaxx::<i64>::esaxx(data.as_ref()).unwrap();
        let sa = esa.sa.into_boxed_slice();
        let sa = sa.iter().map(|&v| {
            v as usize
        }).collect::<Vec<usize>>().into_boxed_slice();

        let (left_elements, right_elements) = ansv::compute_ansv(sa.as_ref());

        let (lpf, prev_occ) = super::lpf_3(data.as_ref(), sa.as_ref(), left_elements.as_ref(), right_elements.as_ref());

        lpf.par_iter().zip(prev_occ.par_iter()).enumerate().for_each(|(idx, (&lpf_val, &prev_occ_idx))| {
            if prev_occ_idx != -1 {
                assert_eq!(super::get_lcp(&data[prev_occ_idx as usize..], &data[idx..]), lpf_val);
            } else {
                assert_eq!(lpf_val, 1);
            }
        });
//
        // This is a proper test of the property that an lpf array is supposed to satisfy -
        // it checks the lcp of every prior suffix and ensures that the calculated lpf is actually the max
        // lcp of the suffix with every prior suffix. Its incredibly slow, so we run it on a random reasonable
        // sampling of the data so we can be at least statistically confident it works!

        // Calculation for sample size: Need p-values below alpha (= 0.05). Can you get a p-value for this?
        // Run it on half the data maybe?

        let mut rng = rand::thread_rng();
        rand::seq::sample_indices(&mut rng, data.len(), utils::DEFAULT_TEST_SAMPLE_SIZE).par_iter().for_each(|&idx| {
            let lpf_val = lpf[idx];
            let suffix = &data[idx..];
            let calculated_lpf = (0..idx).into_par_iter().map(|possible_lpf_ind| {
                super::get_lcp(suffix, &data[possible_lpf_ind..])
            }).max().unwrap_or(1).max(1);
            assert_eq!(calculated_lpf, lpf_val);
        });

    }
    #[test]
    fn test_rayon_pair_chunks() {
        let data = utils::random_slice::<usize>(utils::DEFAULT_TEST_SIZE);
        for chunk_size in 32..256 {
            data.as_ref().par_chunks(chunk_size).enumerate().for_each(|(idx, chunk) | {

                if chunk.len() != chunk_size {
                    assert_eq!(data.last(), chunk.last());
                    assert_eq!(idx, (data.len() + chunk_size - 1)/chunk_size - 1);
                } else {
                    assert_eq!(chunk.len(), chunk_size);
                }
            })
        }

    }
}

