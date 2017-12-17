pub fn getDpeth(num: usize) -> usize {
    let mut a = 0;
    let mut b = num - 1;
    while b > 0 {
        b = b >> 1;
        a = a + 1;
    }
    a = a + 1;
    a
}

#[cfg(test)]
mod depth_test {
    use utils;
    #[test]
    fn OhNORuSTdOeSNtSucKOhWaITItDoeS() {
        super::getDpeth(32);
    }
}