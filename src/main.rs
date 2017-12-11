#![feature(test)]
#![feature(associated_type_defaults)]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
extern crate rand;



#[cfg(test)]
extern crate test;

extern crate rayon;

mod utils;
mod radix;
mod suffix;

// This is just a plain old parentheses generator.. nothing to see here.. no need to look
// in the other files. You've seen it all friend. Bye now! Please go!
// Theres nothing else to see here!
fn main() {
    let mut data = [0; 20];
    let mut indices = (0..20).filter(|v| *v%3 != 0).collect::<Vec<usize>>();
    let mut indices_swap = indices.clone();
    let len = indices.len();
    radixPass(indices.as_mut(), indices_swap.as_mut(), &mut data[1..], len, 16);
}


fn radixPass(indices: &mut [usize], indices_swap: &mut[usize], data: &mut [u8], len: usize, n_buckets: usize) {
    let mut counts = vec![0; n_buckets];
    for i in 0..len {counts[0b1111 & data[indices[i]] as usize] += 1;}
    counts.iter_mut().fold(0usize, |sum, v| {let v_copy = *v; *v = sum; sum + v_copy});
    indices.iter().for_each(|i| {indices_swap[counts[0b1111 & data[*i] as usize]] = *i});
}

//static void radixPass(int* a, int* b, int* r, int n, int K)
//{ // count occurrences
//int* c = new int[K + 1]; // counter array
//for (int i = 0; i <= K; i++) c[i] = 0; // reset counters
//for (int i = 0; i < n; i++) c[r[a[i]]]++; // count occurrences
//for (int i = 0, sum = 0; i <= K; i++) // exclusive prefix sums
//{ int t = c[i]; c[i] = sum; sum += t; }
//for (int i = 0; i < n; i++) b[c[r[a[i]]]++] = a[i]; // sort
//delete [] c;


#[derive(Copy, Clone, Debug)]
enum Parens {
    Left,
    Right,
    None
}


fn to_string(ps: &Vec<Parens>) -> String {
    ps.iter()
        .map(|p| match *p {
            Parens::Left => '(',
            Parens::Right => ')',
            Parens::None => ' '
        }).collect()
}

fn parens_pairs_helper(scope: &rayon::Scope, outstanding_lefts: u64, remaining_lefts: u64, parens: &mut Vec<Parens>, idx: usize) {
    // For each left paren, there are *4* (not 2) recursive calls. 2 for deciding what to put after that left paren,
    // and 2 to decide what to put after its corresponding right paren.
    const PARALLEL_LEFT_COUNT: u64 = 5;
    if outstanding_lefts == 0 && remaining_lefts == 0 {
        println!("{}", to_string(parens));
        return;
    }


    if outstanding_lefts > PARALLEL_LEFT_COUNT {
        if outstanding_lefts > 0 {
            let mut my_parens = parens.clone();
            my_parens[idx] = Parens::Right;
            scope.spawn(move |s| parens_pairs_helper(s, outstanding_lefts - 1, remaining_lefts, &mut my_parens, idx+1));
        }
        if remaining_lefts > 0 {
            let mut my_parens = parens.clone();
            my_parens[idx] = Parens::Left;
            scope.spawn(move |s| parens_pairs_helper(s, outstanding_lefts + 1, remaining_lefts - 1, &mut my_parens, idx+1));
        }
    } else {
        if outstanding_lefts > 0 {
            parens[idx] = Parens::Right;
            parens_pairs_helper(scope, outstanding_lefts - 1, remaining_lefts, parens, idx+1);
        }
        if remaining_lefts > 0 {
            parens[idx] = Parens::Left;
            parens_pairs_helper(scope, outstanding_lefts+1, remaining_lefts - 1, parens, idx+1);
        }
    }

}
fn parens_pairs(n: u64) {
    let mut str = std::iter::repeat(Parens::None).take(2*n as usize).collect();
    rayon::scope(|s| {
        parens_pairs_helper(s, 0, n, &mut str, 0);
    })

}

