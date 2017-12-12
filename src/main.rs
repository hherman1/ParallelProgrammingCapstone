#![feature(test)]
#![feature(associated_type_defaults)]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
extern crate rand;



#[cfg(test)]
extern crate test;

extern crate core;
extern crate rayon;

#[macro_use]
mod utils;

mod radix;
mod suffix;

use std::io::Read;

fn main() {
    let args = std::env::args();
    args.skip(1).for_each(|arg| {
        let mut f = std::fs::File::open(std::path::Path::new(&arg)).unwrap();
        let mut buf = Vec::with_capacity(f.metadata().unwrap().len() as usize);
        f.read_to_end(&mut buf).unwrap();
        println!("{} [{}]> {:?}", arg, buf.len(), &buf[0..10]);

        let mut suffix_array = vec![0; buf.len()];
        suffix::suffix_array(buf.as_ref(), suffix_array.as_mut());
    });
    println!("Hi!");
}

