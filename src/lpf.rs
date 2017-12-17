use rayon::prelude::*;
use rayon;
use utils;

fn LPF_3(data: &[u8], suffix_array: &[usize], longest_previous_factor: &mut [usize], prev_occ: &[usize]) {
    let depth :i32 = 10; //Todo: Change this to real value later
    let ar_len = data.len();

    let left_elements = vec![0; ar_len].into_boxed_slice();
    let right_elements = vec![0; ar_len].into_boxed_slice();

    let rank_array: &mut [usize] = longest_previous_factor;

    //compute ansv here. left_elements and right elements will be populated

    rank_array.par_iter().enumerate().for_each(|(i, &data_i) | {
        let rank_ind_unsafe = rank_array.as_ptr() as *mut usize;
        unsafe {
            *rank_ind_unsafe.offset(data_i as isize) = i;
        }
    });

    let num_threads = 2 * rayon::current_num_threads(); // Figure out why paper multiplies this by 2 here

    let size = (ar_len + num_threads - 1) / num_threads;







}






//
//
//#[cfg(test)]
//mod LPF_testing {
//    use utils;
//    #[test]
//    fn rustSucks_aLotYoUAredwsxceatgrvefgV5T4FEVENuSinGsnaJeCaSE() {
//
//        let data = utils::random_slice(32);
//        let suf_ar = utils::random_slice_usize(32);
//        let mut lpf = utils::random_slice(32);
//        let mut prev_occ = utils::random_slice(32);
//
//        super::LPF_3(data.as_ref(), suf_ar.as_ref(), lpf.as_mut(), prev_occ.as_mut());
//    }
//}