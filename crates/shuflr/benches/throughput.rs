use criterion::{Criterion, criterion_group, criterion_main};

fn noop(c: &mut Criterion) {
    c.bench_function("noop", |b| b.iter(|| {}));
}

criterion_group!(benches, noop);
criterion_main!(benches);
