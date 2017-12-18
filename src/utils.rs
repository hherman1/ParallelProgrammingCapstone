use std;

#[cfg(test)]
use rand;
#[cfg(test)]
use rand::Rng;


#[macro_export]
macro_rules! dbg {
    ($first:expr $(, $var:expr)* ) => {{
        #[cfg(debug_assertions)]
        {
            print!("{}:{}> {:?}", file!(), line!(), $first);
            $({
                print!(", {:?}", $var);
            })*
            println!();
        }
    }}
}

// See izip in itertools source code for further explanation
#[macro_export]
macro_rules! generic_izip {
    (@make_closure $argument:pat => $result:expr ; ) => {
        |$argument| $result
    };
    (@make_closure $argument:pat => ( $($arg:tt)* ) ; $_next:expr $(, $iter:expr)*) => {
        generic_izip!(@make_closure ($argument, b) => ( $($arg)*, b ) ; $($iter),*)
    };
    ($first:expr $(, $iter:expr)*) => {{
        $first
        $(.zip($iter))*
        .map(generic_izip!(@make_closure a => (a) ; $($iter),*))
    }};
}

// Consts

pub const BENCH_SIZE: usize = 65536 * 8;
pub const DEFAULT_TEST_SIZE: usize = 65536;


#[cfg(test)]
pub fn random_slice<T: rand::Rand>(len:usize) -> Box<[T]> {
    let mut rng = rand::thread_rng();
    rng.gen_iter().take(len).collect::<Vec<T>>().into_boxed_slice()
}

#[cfg(test)]
pub fn random_slice_with_zeroes(len: usize) -> Box<[u8]> {
    let mut res = random_slice(len);
    res.iter_mut().rev().take(2).for_each(|v| *v = 0);
    res
}

pub fn to_suffix_triplet_slice(data: &[u8]) -> Box<[[u8; 3]]> {
    (0..data.len()-2).map(|i| {
        [data[i], data[i + 1], data[i + 2]]
    }).collect::<Vec<[u8; 3]>>().into_boxed_slice()
}

pub fn triplet_slice(data: Vec<u8>) -> Box<[[u8; 3]]> {
    let mut res = Vec::<[u8; 3]>::with_capacity(data.len() / 3);
    for i in 0..data.len() / 3 {
        res.push([data[3 * i], data[3 * i + 1], data[3 * i + 2]]);
    }
    res.into_boxed_slice()
}

#[cfg(test)]
pub fn random_triplet_slice(len: usize) -> Box<[[u8; 3]]> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; len * 3];
    rng.fill_bytes(data.as_mut_slice());
    triplet_slice(data)
}

//Manipulation

pub fn multi_split_mut_slice<'a, T>(slice: &'a mut [T], bounds: &[usize]) -> Box<[&'a mut [T]]>{
    let mut out:Vec<&mut [T]> = Vec::with_capacity(bounds.len());
    bounds.iter().fold((0usize, slice), |(prev_bound, rem_slice), &bound| {
        let (tmp_slice, new_rem_slice) = rem_slice.split_at_mut(bound - prev_bound);
        out.push(tmp_slice);
        (bound, new_rem_slice)
    });
    out.into_boxed_slice()
}
pub fn multi_split_slice<'a, T>(slice: &'a [T], bounds: &[usize]) -> Box<[&'a [T]]>{
    let mut out:Vec<&[T]> = Vec::with_capacity(bounds.len());
    bounds.iter().fold((0usize, slice), |(prev_bound, rem_slice), &bound| {
        let (tmp_slice, new_rem_slice) = rem_slice.split_at(bound - prev_bound);
        out.push(tmp_slice);
        (bound, new_rem_slice)
    });
    out.into_boxed_slice()
}

pub fn bounds_for_num_chunks_and_chunk_size(data_len: usize, chunk_size: usize, n_chunks: usize) -> Box<[usize]> {
    (0usize..n_chunks).map(|bound| ((bound+1)*chunk_size).min(data_len))
        .collect::<Vec<usize>>().into_boxed_slice()
}
pub fn bounds_for_chunk_size(data_len: usize, chunk_size: usize) -> Box<[usize]> {
    bounds_for_num_chunks_and_chunk_size(data_len, chunk_size,((data_len as f64)/(chunk_size as f64)).ceil() as usize)
}
pub fn bounds_for_num_chunks(data_len: usize, num_chunks: usize) -> Box<[usize]> {
    bounds_for_num_chunks_and_chunk_size(data_len,((data_len as f64)/(num_chunks as f64)).ceil() as usize, num_chunks)
}

pub fn chunk_mut_slice<T>(slice: &mut [T], chunk_size: usize) -> Box<[&mut [T]]> {
    let len = slice.len();
    multi_split_mut_slice(slice, bounds_for_chunk_size(len, chunk_size).as_ref())
}
pub fn chunk_slice<T>(slice: & [T], chunk_size: usize) -> Box<[& [T]]> {
    let len = slice.len();
    multi_split_slice(slice, bounds_for_chunk_size(len, chunk_size).as_ref())
}

pub fn n_split_mut_slice<T>(slice: &mut [T], n: usize) -> Box<[&mut [T]]> {
    let len = slice.len();
    multi_split_mut_slice(slice, bounds_for_num_chunks(len, n).as_ref())
}
pub fn n_split_slice<T>(slice: & [T], n: usize) -> Box<[& [T]]> {
    let len = slice.len();
    multi_split_slice(slice, bounds_for_num_chunks(len, n).as_ref())
}

pub struct SyncUnsafeCell<T>(std::cell::UnsafeCell<T>);
unsafe impl<T> Sync for SyncUnsafeCell<T> {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn chunk_slice_test() {
        let mut data = random_slice::<u8>(30);
        let data_copy = data.clone();
        let chunk_size = 30 / 8;
        let chunked = super::chunk_mut_slice(data.as_mut(), chunk_size);
        assert_eq!(chunked.len(), 10);
        chunked.iter().enumerate().for_each(|(idx, chunk)| {
            assert_eq!(data_copy[3*idx + 0], chunk[0]);
            assert_eq!(data_copy[3*idx + 1], chunk[1]);
            assert_eq!(data_copy[3*idx + 2], chunk[2]);
        })

    }
}


