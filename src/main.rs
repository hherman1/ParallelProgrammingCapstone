// Ideas:
// 1. Ship binarys for Widnows, Mac, and Linux along with test data which
// we can make available through GitHub's releases section, and can advertise
// to the class to download and try out during our presentation... we'll want to warn them
// while we're setting up that they should get their computers out if they want to try our finished
// project.
// We also need to give instructions on how to verify the results, that they can use `unzip`
// to undo the compressed files. We can put full demo instructions in the repo's readme.
// Hopefully there will be at least a couple people who actually want to try it.
// 2. We can't cover all the tricks in detail in time. We should f ocus on whats the problem and
// the central idea.
// 3. We should cite the 3 main papers we used (1 PLZ77 paper , 2 suffix array papers);


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

