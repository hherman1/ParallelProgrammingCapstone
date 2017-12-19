pub fn get_depth(num: usize) -> usize {
    let mut a = 0;
    let mut b = num - 1;
    while b > 0 {
        b = b >> 1;
        a = a + 1;
    }
    a = a + 1;
    a
}

fn lpf_to_lz(lpf : Box<[usize]>, prev_occ : Box<[isize]>, ar_len : usize) {

}