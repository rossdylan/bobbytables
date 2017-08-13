#![feature(test)]
#![feature(try_from)]

extern crate test;
extern crate rand;

mod state;
mod table;
mod key;
mod counter;
mod iter;


//
//#[cfg(test)]
//mod tests {
//    use std::thread;
//    use std::sync::Arc;
//    use test::Bencher;
//    use super::*;
//
//    #[test]
//    fn sequential_iter() {
//        let counter = Counter::new();
//        for i in 0..20 {
//            let key = format!("key_{0}", i);
//            counter.incr(&key, 1);
//        }
//        for (key, val) in (&counter).into_iter() {
//            println!("k: {}, v: {}", key, val);
//            assert_eq!(val, 1)
//        }
//    }
//
//    #[test]
//    fn concurrent_iter() {
//        let counter = Counter::new();
//        let shared = Arc::new(counter);
//        let nthreads = 8;
//        let c = shared.clone();
//        for i in 0..20 {
//            let key = format!("key_{0}", i);
//            c.incr(&key, 1);
//        }
//        let mut children = vec![];
//        for _ in 0..nthreads {
//            let c = shared.clone();
//            children.push(thread::spawn(move|| {
//                for (_, val) in (&c).into_iter() {
//                    assert_eq!(val, 1);
//                }
//            }));
//        }
//        for t in children {
//            let _ = t.join();
//        }
//    }
//
//    #[test]
//    fn concurrent_iter_and_incr() {
//        let counter = Counter::new();
//        let shared = Arc::new(counter);
//        let mut children = vec![];
//        let c1 = shared.clone();
//        children.push(thread::spawn(move|| {
//            for _ in 0..200 {
//                for (key, _) in (&c1).into_iter() {
//                    assert_eq!(key, "key_1")
//                }
//            }
//        }));
//        let c2 = shared.clone();
//        children.push(thread::spawn(move|| {
//            for _ in 0..10000 {
//                c2.incr("key_1", 1);
//            }
//        }));
//
//        for t in children {
//            let _ = t.join();
//        }
//    }
//
//    #[bench]
//    fn bench_into_iter(b: &mut Bencher) {
//        let counter = Counter::new();
//        let key = format!("key_{0}", 1);
//        counter.incr(&key, 1);
//        b.iter(|| (&counter).into_iter())
//    }
//
//    #[bench]
//    fn bench_iter(b: &mut Bencher) {
//        let counter = Counter::new();
//        let key = format!("key_{0}", 1);
//        counter.incr(&key, 1);
//        b.iter(|| {
//            for (key, val) in counter.into_iter() {
//                drop(key);
//                drop(val);
//            }
//        })
//
//    }
//}
