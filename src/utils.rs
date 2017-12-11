use rand;
use rand::Rng;
use std::ops::DerefMut;

pub const BENCH_SIZE: usize = 65536 * 8;

pub fn random_slice(len: usize) -> Box<[u8]> {
    let mut rng = rand::thread_rng();
    let mut out = vec![0u8; len].into_boxed_slice();
    rng.fill_bytes(out.as_mut());
    out
}

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

pub fn random_triplet_slice(len: usize) -> Box<[[u8; 3]]> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; len * 3];
    rng.fill_bytes(data.as_mut_slice());
    triplet_slice(data)
}
