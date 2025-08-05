use std::time::UNIX_EPOCH;

pub fn time_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("oops")
        .as_micros() as u64
}
