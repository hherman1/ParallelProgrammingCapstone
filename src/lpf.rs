use rayon::prelude::*;
use rayon;
use utils;
use std::convert::AsMut;
use serial_suffix;
use ansv;

fn lpf_3(data: &[u8], suffix_array: &[usize]) -> (Box<[usize]>, Box<[isize]>) {
    let depth :i32 = 10; //Todo: Change this to real value later
    let ar_len = data.len();

    let mut longest_previous_factor = vec![0usize; ar_len].into_boxed_slice();
    let mut prev_occ = vec![0isize; ar_len].into_boxed_slice();


    let (mut left_elements, mut right_elements) = ansv::compute_ansv(suffix_array);

    let mut left_lpf :Box<[isize]> = vec![0; ar_len].into_boxed_slice();
    let mut right_lpf :Box<[isize]> = vec![0; ar_len].into_boxed_slice();

    let mut rank_array = longest_previous_factor;
    suffix_array.par_iter().enumerate().for_each(|(i, &data_i)| {
        unsafe {
            *(rank_array.as_ptr() as *mut usize).offset(data_i as isize) = i;
        }
    });

    let mut longest_previous_factor = rank_array;


    let num_threads = 2 * rayon::current_num_threads(); // Figure out why paper multiplies this by 2 here

    let size = utils::rayon_chunk_size(ar_len);


    generic_izip!(
        longest_previous_factor.as_mut().par_chunks_mut(size),
        AsMut::<[isize]>::as_mut(&mut left_lpf).par_chunks_mut(size),
        AsMut::<[isize]>::as_mut(&mut right_lpf).par_chunks_mut(size),
        prev_occ.as_mut().par_chunks_mut(size))
        .enumerate()
        .for_each(|(idx, (lpf_chunk, left_lpf_chunk, right_lpf_chunk, prev_occ_chunk))| {
            //data[i] -> data[idx * size]

            let mut abs_start_pos :usize = idx * lpf_chunk.len();

            let rank = *lpf_chunk.first().unwrap();

            //Compute lpf for first element
            let mid :usize = rank;
            let left :isize = left_elements[rank];
            let right :isize = right_elements[rank];

            let mut llcp = 0;
            let mut rlcp = 0;


            //Todo These if statements work 50% of the time. When they fail they get "thread '<unnamed>' panicked at 'index out of bounds: the len is 32 but the index is 32'"
            if left != -1 {
                while data[suffix_array[left as usize] + llcp] == data[abs_start_pos + llcp] {
                    llcp = llcp + 1;
                    if llcp >= data.len() {
                        dbg!(llcp, abs_start_pos, idx, suffix_array);
                    }
                }
            } else {
                *left_lpf_chunk.first_mut().unwrap() = 0;
            }
//
//            if right != -1 {
//                while data[suffix_array[right as usize] + rlcp] == data[abs_start_pos + rlcp]{
//                    rlcp = rlcp + 1;
//                }
//            } else {
//                *right_lpf_chunk.first_mut().unwrap() = 0;
//            }
//
//            if *left_lpf_chunk.first().unwrap() == 0 && *right_lpf_chunk.first().unwrap() == 0 {
//                *prev_occ_chunk.first_mut().unwrap() = -1;
//                *lpf_chunk.first_mut().unwrap() = 1;
//            } else if *left_lpf_chunk.first().unwrap() > *right_lpf_chunk.first().unwrap() {
//                *prev_occ_chunk.first_mut().unwrap() = suffix_array[left as usize] as isize;
//                *lpf_chunk.first_mut().unwrap() = *left_lpf_chunk.first().unwrap() as usize;
//            } else {
//                *prev_occ_chunk.first_mut().unwrap() = suffix_array[right as usize] as isize;
//                *lpf_chunk.first_mut().unwrap() = *right_lpf_chunk.first().unwrap() as usize;
//            }

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
    #[test]
    fn changed_this_name_so_idea_would_let_me_commit_thanks_mack_hartley_whose_social_security_number_is_123_45_6789() {
        let data = utils::random_slice(utils::DEFAULT_TEST_SIZE);
        let esa = saxx::Esaxx::<i64>::esaxx(data).unwrap();
        let sa = esa.sa.into_boxed_slice();
        let sa = sa.iter().map(|&v| {
            v as usize
        }).collect::<Vec<usize>>().into_boxed_slice();


        //        serial_suffix::SuffixTable::new(data);

        let mut suf_ar = (0usize..32).collect::<Vec<usize>>().into_boxed_slice();

        super::lpf_3(data.as_ref(), suf_ar.as_ref());
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