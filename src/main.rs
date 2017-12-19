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
#![feature(unique)]
#![feature(allocator_api)]
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
extern crate suffix as serial_suffix;
extern crate saxx;
extern crate clap;

#[macro_use]
extern crate lazy_static;

#[macro_use]
mod utils;

mod radix;
mod suffix;
mod ansv;
mod lpf;
mod lpf_to_lz;

use std::io::Read;

use rayon::prelude::*;

use std::sync::Mutex;
use std::collections::HashMap;
lazy_static! {
    static ref STATS : Mutex<HashMap<&'static str, f64>> = {
        let mut hash = HashMap::new();
        Mutex::new(hash)
    };
}

fn float_secs(d: std::time::Duration) -> f64 {
    (d.as_secs() as f64) + (d.subsec_nanos() as f64)/1e9f64
}

// Updates time and returns elapsed time ... difference between old and new.
fn tick(time: &mut std::time::Instant) -> std::time::Duration {
    let new_time = std::time::Instant::now();
    let res = new_time - *time;
    *time = new_time;
    res
}

fn lempel_ziv_77(data: &[u8]) -> Box<[usize]> {
    let mut time = std::time::Instant::now();

    let esa = saxx::Esaxx::<i64>::esaxx(data.as_ref()).unwrap();

    STATS.lock().unwrap().insert("esa_runtime", float_secs(tick(&mut time)));

    let sa = esa.sa.into_boxed_slice();
    let sa = sa.iter().map(|&v| {
        v as usize
    }).collect::<Vec<usize>>().into_boxed_slice();

    let (left_elements, right_elements) = ansv::compute_ansv(sa.as_ref());

    STATS.lock().unwrap().insert("ansv_runtime", float_secs(tick(&mut time)));

    let (lpf, prev_occ) = lpf::lpf_3(data.as_ref(), sa.as_ref(), left_elements.as_ref(), right_elements.as_ref());

    STATS.lock().unwrap().insert("lpf_runtime", float_secs(tick(&mut time)));

    let out = lpf_to_lz::lpf_to_lz_serial(lpf.as_ref());

    STATS.lock().unwrap().insert("lpf_to_lz_runtime", float_secs(tick(&mut time)));

    out

}

fn main() {
    let matches = clap::App::new("gRip 2.X: Parallel Lempel-Ziv 77 Implementation")
        .version("0.0.0.0.0.0.1")
        .author("Mack Hartley & Hunter Herman")
        .about("Calculates Lempel Ziv factorization, and reports info about it.")
        .arg(clap::Arg::with_name("print")
            .short("p")
            .help("Print the final Lempel-Ziv factorization."))
        .arg(clap::Arg::with_name("stats")
            .short("s")
            .multiple(true)
            .help("Print stats about the factorization."))
        .arg(clap::Arg::with_name("INPUT")
            .required(true)
            .index(1)
            .help("Sets the file to factorize."))
        .arg(clap::Arg::with_name("n-threads")
            .short("np")
            .help("Sets the number of threads to calculate with.")
            .takes_value(true)
            .long("num-threads"))
        .get_matches();


    let filename = matches.value_of("INPUT").unwrap();
    let stats_level = matches.occurrences_of("stats");
    let should_print = matches.is_present("print");
    let num_threads_opt = matches.value_of("n-threads");


    let start = std::time::Instant::now();

    let mut f = std::fs::File::open(std::path::Path::new(&filename)).unwrap();
    let mut buf = Vec::with_capacity(f.metadata().unwrap().len() as usize);
    f.read_to_end(&mut buf).unwrap();


    let lz = match num_threads_opt {
        Some(num_threads_str) => {
            let num_threads: usize = num_threads_str.parse().unwrap();

            let tp = rayon::Configuration::new()
                .num_threads(num_threads)
                .build().unwrap();

            tp.install(|| lempel_ziv_77(buf.as_ref()))
        }
        None => lempel_ziv_77(buf.as_ref())
    };

    let total_run_time = std::time::Instant::now() - start;

    println!("<FINISHED>");
    if stats_level > 0 {
        println!("Compressed {} bytes in {}s.", buf.len(), float_secs(total_run_time));

        if stats_level > 1 {
            println!("-- Finished phase `{}` in {}s", "Suffix Array", STATS.lock().unwrap().get("esa_runtime").unwrap_or(&-1f64));
            println!("-- Finished phase `{}` in {}s", "ANSV Arrays", STATS.lock().unwrap().get("ansv_runtime").unwrap_or(&-1f64));
            println!("-- Finished phase `{}` in {}s", "LPF Array", STATS.lock().unwrap().get("lpf_runtime").unwrap_or(&-1f64));
            println!("-- Finished phase `{}` in {}s", "LPF Array To LZ Array", STATS.lock().unwrap().get("lpf_to_lz_runtime").unwrap_or(&-1f64));
            println!();
        }

        println!("Approximate output length: {}", lz.len());
        println!("Approximate reduction ratio: {}", (buf.len() as f64)/(lz.len() as f64));
    }
    if stats_level > 1 {
        let average_reduction_factor = (lz.par_iter().zip(lz.par_iter().skip(1))
            .map(|(&l, &r)| r - l).sum::<usize>() as f64)/(lz.len() as f64 - 1f64);
        println!("Average pattern length: {}", average_reduction_factor);
    }
     if should_print {
         println!("<FACTORIZATION>");
         println!();
         println!("{:?}", lz);

     }

}

#[cfg(test)]
mod suffix_testing {
    use serial_suffix::SuffixTable;
    use saxx;
    use utils;
    use test;
    #[test]
    fn test_suffix() {
        let st = SuffixTable::new("the quick brown fox was quick.");
        assert_eq!(st.positions("quick"), &[4, 24]);

        // Or if you just want to test existence, this is faster:
        assert!(st.contains("quick"));
        assert!(!st.contains("faux"));
    }
    #[bench]
    fn saxx_bench(bencher: &mut test::Bencher) {
        let data = utils::random_slice::<u8>(utils::BENCH_SIZE);
        bencher.iter(|| {
            saxx::Esaxx::<i64>::esaxx(data.as_ref()).unwrap();
        })
    }
    #[test]
    fn saxx_test() {
        let data = utils::random_slice::<u8>(utils::DEFAULT_TEST_SIZE);
        let esa = saxx::Esaxx::<i32>::esaxx(data.as_ref()).unwrap();
        let sa = esa.sa.into_boxed_slice();

    }

    #[bench]
    fn lempel_ziv_77_bench(bencher: &mut test::Bencher) {
        let data = utils::random_slice::<u8>(utils::BENCH_SIZE);
        bencher.iter(|| {
            super::lempel_ziv_77(data.as_ref());
        })
    }
}
