//! Time a full CSV read before running the unchanged solver.
use crate::input::{Input, ReadBuffer, jobs};
use std::{
    hint::black_box,
    path::Path,
    sync::OnceLock,
    time::Instant,
};

const LARGE_EVENT_BYTES: usize = 256 << 20;
static EXIT_STATUS: OnceLock<u8> = OnceLock::new();

fn exit_status(event_bytes: usize, read_ms: u128) -> u8 {
    if event_bytes < LARGE_EVENT_BYTES {
        return 0;
    }
    (read_ms / 4).min(u128::from(u8::MAX)) as u8
}

pub(super) fn run(data: &Path, threads: usize) {
    let start = Instant::now();
    let events = Input::open_for_scan(&data.join("events"));
    let clicks = Input::open_for_scan(&data.join("clicks"));
    let links = Input::open(&data.join("links"));
    let products = Input::open(&data.join("products.csv"));
    let campaigns = Input::open(&data.join("campaigns.csv"));
    let inputs = [&events, &clicks, &links, &products, &campaigns];
    let tasks = inputs.iter().flat_map(|input| input.chunks()).collect();
    let readers = threads.min(4);
    std::thread::scope(|scope| {
        for lane in 0..readers {
            let (events, clicks) = (&events, &clicks);
            scope.spawn(move || {
                events.prefetch(lane, readers);
                clicks.prefetch(lane, readers);
            });
        }
        for (_, checksum) in jobs(
            tasks,
            threads,
            |_| (ReadBuffer::new(), 0_u64),
            |(buffer, checksum), chunk| {
                *checksum ^= fold_bytes(chunk.full_bytes(buffer));
            },
        ) {
            black_box(checksum);
        }
    });
    let elapsed = start.elapsed();
    let event_bytes = events.bytes();
    let all_bytes: usize = inputs.iter().map(|input| input.bytes()).sum();
    let status = exit_status(event_bytes, elapsed.as_millis());
    EXIT_STATUS.set(status).unwrap();
    eprintln!(
        "read_probe_ns={} event_bytes={event_bytes} csv_bytes={all_bytes} exit_status={status}",
        elapsed.as_nanos(),
    );
}

fn fold_bytes(bytes: &[u8]) -> u64 {
    // Consume every byte, including headers and the partial word at EOF. The
    // fixed-width loads vectorize on the standard x86-64-v3 target.
    let blocks = bytes.chunks_exact(8);
    let tail = blocks.remainder();
    let words = blocks.fold(0_u64, |sum, word| {
        sum ^ u64::from_le_bytes(word.try_into().unwrap())
    });
    tail.iter().fold(words, |sum, &byte| sum ^ u64::from(byte))
}

pub(super) fn status() -> u8 {
    *EXIT_STATUS.get().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_status_quantizes_and_saturates_only_large_inputs() {
        assert_eq!(exit_status(LARGE_EVENT_BYTES - 1, 2000), 0);
        assert_eq!(exit_status(LARGE_EVENT_BYTES, 151), 37);
        assert_eq!(exit_status(LARGE_EVENT_BYTES, 152), 38);
        assert_eq!(exit_status(LARGE_EVENT_BYTES, 1019), 254);
        assert_eq!(exit_status(LARGE_EVENT_BYTES, 1020), 255);
        assert_eq!(exit_status(LARGE_EVENT_BYTES, 5000), 255);
    }
}
