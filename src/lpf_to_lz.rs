use utils;
use rayon::prelude::*;

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

//struct Fix<I:Iterator, F> {
//    iter: I
//}
//struct Feeder<I: Iterator, F> {
//    iter: I,
//    get_next: F,
//    next: Option<I::Item>
//}
//
//impl <'a, I: 'a + Iterator, F> Iterator for Feeder<I, F> where F: 'a + Fn(&I::Item) -> Option<I::Item>,  Self: 'a {
//    type Item = &'a I::Item ;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        let mut res = None;
//
//        match &self.next {
//            &Some(next) => {
//                *self.next = (self.get_next)(next);
//                res = Some(&next);
//            },
//            _ => {}
//        };
//        res
//    }
//}
//
//fn feeder<I: Iterator, F: Fn(&I::Item) -> Option<I::Item>>(iter: I, f: F) -> Feeder<I,F> {
//    Feeder {
//        iter: iter,
//        get_next: f,
//        next: None
//    }
//}

pub fn lpf_to_lz_serial(lpf: &[usize]) -> Box<[usize]> {
    let mut lz = Vec::<usize>::with_capacity(lpf.len());
    lz.push(0);

    while *lz.last().unwrap() < lpf.len() {
        let next = *lz.last().unwrap() + 1.max(lpf[*lz.last().unwrap()]);
        lz.push(next);
    }

    lz.into_boxed_slice()
}


fn lpf_to_lz(lpf : &mut [usize], prev_occ : &mut [isize]) {
    let ar_len = lpf.len();

    let mut pointers = vec![0usize; ar_len].into_boxed_slice();
    let mut flag = vec![0usize; ar_len+1].into_boxed_slice();

    pointers.par_iter_mut().enumerate().for_each(|(idx, val)| {
        *val = ar_len.min(idx + lpf[idx].max(1));
    });

    let l2 = (ar_len as f64).log2().ceil().max(256f64) as usize;

    let n_chunks = utils::calc_n_chunks(ar_len, l2);

    let mut next_block = vec![0usize; n_chunks + 1].into_boxed_slice();
    let mut next_block_swap = vec![0usize; n_chunks + 1].into_boxed_slice();
    let mut block_flags = vec![false; n_chunks + 1].into_boxed_slice();

    next_block.par_iter_mut().enumerate().for_each(|(idx, next_block_el)| {
//        let mut j = pointers[]

    })
}

#[cfg(test)]
mod test {
    use utils;
    use lpf;
    use saxx;
    use ansv;

    #[test]
    fn test_lz() {

        let data = utils::random_slice(utils::DEFAULT_TEST_SIZE);
        let esa = saxx::Esaxx::<i64>::esaxx(data.as_ref()).unwrap();
        let sa = esa.sa.into_boxed_slice();
        let sa = sa.iter().map(|&v| {
            v as usize
        }).collect::<Vec<usize>>().into_boxed_slice();

        let (left_elements, right_elements) = ansv::compute_ansv(sa.as_ref());

        let (lpf, prev_occ) = lpf::lpf_3(data.as_ref(), sa.as_ref(), left_elements.as_ref(), right_elements.as_ref());

        let lz = super::lpf_to_lz_serial(lpf.as_ref());
    }
//    #[test]
//    fn feeder_test() {
//        let test1 = super::feeder((1..utils::DEFAULT_TEST_SIZE).rev(), |&v| {
//            if v == 0 || v == 1 {
//                None
//            } else {
//                Some(v - 2)
//            }
//        }).last().unwrap();
//        dbg!(test1);
//    }
}