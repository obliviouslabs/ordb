use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ordb::ObliviousDB;
use rayon::vec;
use serde::de::value;

fn benchmark_db_insert_small_kv(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 8;
    const VALUE_SIZE: usize = 8;
    const DB_SIZE: usize = 10000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value = vec![0; VALUE_SIZE];
        db.insert(key.to_vec(), value.to_vec());
    }
    c.bench_function("db_insert", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            let value = vec![0; VALUE_SIZE];
            db.insert(key.to_vec(), value.to_vec());
        })
    });
}

fn benchmark_db_insert_large_kv(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 32;
    const VALUE_SIZE: usize = 512;
    const DB_SIZE: usize = 100000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value = vec![0; VALUE_SIZE];
        db.insert(key.to_vec(), value.to_vec());
    }
    c.bench_function("db_insert_large", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            let value = vec![0; VALUE_SIZE];
            db.insert(key.to_vec(), value);
        })
    });
}

fn benchmark_db_insert_varied_val(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 32;
    const DB_SIZE: usize = 100000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value_size = rand::random::<usize>() % 512;
        let value = vec![0; value_size];
        db.insert(key.to_vec(), value);
    }
    c.bench_function("db_insert_varied", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            let value_size = rand::random::<usize>() % 512;
            let value = vec![0; value_size];
            db.insert(key.to_vec(), value);
        })
    });
}

fn benchmark_db_get_100k(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 32;
    const VALUE_SIZE: usize = 512;
    const DB_SIZE: usize = 100000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value = vec![0; VALUE_SIZE];
        db.insert(key.to_vec(), value);
    }
    c.bench_function("db_get_100k", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            db.get(&key);
        })
    });
}

fn benchmark_db_get_1m(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 32;
    const VALUE_SIZE: usize = 512;
    const DB_SIZE: usize = 1000000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value = vec![0; VALUE_SIZE];
        db.insert(key.to_vec(), value);
    }
    c.bench_function("db_get_1m", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            db.get(&key);
        })
    });
}

fn benchmark_db_get_10m_small_val(c: &mut Criterion) {
    let db = ObliviousDB::new();
    const KEY_SIZE: usize = 32;
    const VALUE_SIZE: usize = 32;
    const DB_SIZE: usize = 10000000;
    for _ in 0..DB_SIZE {
        let key = rand::random::<[u8; KEY_SIZE]>();
        let value = vec![0; VALUE_SIZE];
        db.insert(key.to_vec(), value);
    }
    c.bench_function("db_get_10m", |b| {
        b.iter(|| {
            let key = rand::random::<[u8; KEY_SIZE]>();
            db.get(&key);
        })
    });
}

criterion_group!(
    benches,
    benchmark_db_insert_small_kv,
    benchmark_db_insert_large_kv,
    benchmark_db_insert_varied_val,
    benchmark_db_get_100k,
    benchmark_db_get_1m,
    benchmark_db_get_10m_small_val
);
criterion_main!(benches);
