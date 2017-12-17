use rayon::prelude::*;
use rayon;
use utils;

fn LPF_3(data: &[u8], suffix_array: &[usize], longest_previous_factor: &mut [usize], prev_occ: &[usize]) {
    let depth :i32 = 10; //Todo: Change this to real value later
    let ar_len = data.len();

    let left_elements = vec![0; ar_len].into_boxed_slice();
    let right_elements = vec![0; ar_len].into_boxed_slice();

    let left_lpf = vec![0; ar_len].into_boxed_slice();
    let right_lpf = vec![0; ar_len].into_boxed_slice();

    let rank_array: &mut [usize] = longest_previous_factor;

    //compute ansv here. left_elements and right elements will be populated

    suffix_array.par_iter().enumerate().for_each(|(i, &data_i) | {
        let rank_ind_unsafe = rank_array.as_ptr() as *mut usize;
        unsafe {
            *rank_ind_unsafe.offset(data_i as isize) = i;
        }
    });

    let num_threads = 2 * rayon::current_num_threads(); // Figure out why paper multiplies this by 2 here

    let size = (ar_len + num_threads - 1) / num_threads;

    generic_izip!(
        utils::chunk_mut_slice(longest_previous_factor, size).par_iter_mut(),
        utils::chunk_mut_slice(left_lpf, size).par_iter_mut(),
        utils::chunk_mut_slice(right_lpf, size).par_iter_mut(),
        utils::chunk_mut_slice(prev_occ, size).par_iter_mut())
        .enumerate()
        .for_each(|(idx, (lpf_chunk, left_lpf_chunk, right_lpf_chunk, prev_occ_chunk))| {
            //data[i] -> data[idx * size]

            let mut abs_position = idx * lpf_chunk.len();

            //Compute lpf for first element
            let mid = rank_array[abs_position];
            let left = left_elements[rank_array[abs_position]];
            let right = right_elements[rank_array[abs_position]];

            let mut llcp = 0;
            let mut rlcp = 0;


            //Todo These three iffs stil need to be checked to see if they work
            if left != -1 {
                while data[suffix_array[left] + llcp] == data[abs_position + llcp] {
                    llcp = llcp + 1;
                }
            } else {
                left_lpf_chunk[idx] = 0;
            }

            if right != -1 {
                while data[suffix_array[right] + rlcp] == data[abs_position + rlcp]{
                    rlcp = rlcp + 1;
                }
            } else {
                right_lpf_chunk[idx] = 0;
            }

            if left_lpf[abs_position] == 0 && right_lpf[abs_position] == 0 {
                prev_occ_chunk[idx] = -1;
                lpf_chunk[idx] = 1;
            }

            // no neighbor

        });

}








#[cfg(test)]
mod LPF_testing {
    use utils;
    use rand;
    use rand::*;
    #[test]
    fn rustSucks_aLotYoUAredwsxceatgrvefgV5T4FEVENuSinGsnaJeCaSE() {

        let mut suf_ar = (0usize..32).collect::<Vec<usize>>().into_boxed_slice();
        let mut rng = rand::thread_rng();
        rng.shuffle(suf_ar.as_mut());

        let data = utils::random_slice(32);
        let mut lpf = utils::random_slice(32);
        let mut prev_occ = utils::random_slice(32);

        super::LPF_3(data.as_ref(), suf_ar.as_ref(), lpf.as_mut(), prev_occ.as_mut());
    }
}