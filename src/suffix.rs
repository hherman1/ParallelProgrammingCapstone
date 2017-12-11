use radix;
use rayon::prelude::*;

// Data must end with two 0 entries which are not used
fn suffix_array(data: & [u8], suffix_array: &mut [usize]) {
    println!("test");

    let mut mod_3_indices = (0usize..data.len()-2).filter(|v| v%3 != 0).collect::<Vec<usize>>();
    radix::par_radix_triplet_indices_sort(data, mod_3_indices.as_mut());

    println!("test");

    let uniques: usize = mod_3_indices.par_iter()
        .zip(mod_3_indices.par_iter().skip(1))
        .map(|(&i, &j)| {
            (data[i] != data[j] ||
                data[i+1] != data[j+1] ||
                data[i+2] != data[j+2])
                as usize
        })
        .sum();
//    let uniques: Vec<usize> = mod_3_indices.par_iter()
//        .zip(mod_3_indices.par_iter().skip(1))
//        .filter(|(&i, &j)| {
//            data[i] != data[j] ||
//                data[i+1] != data[j+1] ||
//                data[i+2] != data[j+2])
//        })
//        .map(|(&i, &j))

//    let mut partitioned_indices = vec!
    mod_3_indices.par_iter().for_each(|&i| {

    });

    println!("{}", uniques);
    if uniques < mod_3_indices.len() {
        
    }

//    (0..mod_3_indices.len()).par_iter()
//        .map(|i)
}

#[derive(Debug)]
enum CharType {
    S, L, LMS
}

fn suffix_array_2(data: & [u8], suffix_array: &mut [usize]) {
    let mut types = vec![CharType::S; data.len()].into_boxed_slice();
    data.iter().rev().zip(types.iter_mut().rev())
        .fold((0u8, CharType::S), |(next_item, next_type), (&cur_item, cur_type )| {
           if cur_item == next_item {
               *cur_type = next_type;
           } else if cur_item < next_item {
               *cur_type = match next_item {
                   LMS => S,
                   S => S,
                   L => LMS
               }
           } else {
               *cur_type = match next_item {
                   LMS => S,
                   S => S,
                   L => LMS
               }
           }
        })


}

#[cfg(test)]
mod test {
    use utils::*;
    #[test]
    fn suffix_array_test() {
        println!("33");
        let x = random_slice_with_zeroes(BENCH_SIZE);
        let mut suffix_array = vec![0; BENCH_SIZE];
        super::suffix_array(x.as_ref(), suffix_array.as_mut());

    }
}