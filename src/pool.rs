use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::Relaxed},
    mpsc::{self, SyncSender},
};

type Job<'scope> = Box<dyn FnOnce() + Send + 'scope>;

pub(super) struct Pool<'scope> {
    channels: Vec<SyncSender<Job<'scope>>>,
    threads: usize,
}

impl<'scope> Pool<'scope> {
    pub(super) fn new<'env>(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        threads: usize,
        enabled: bool,
    ) -> Self {
        let mut channels = Vec::new();
        if enabled {
            for _ in 0..threads {
                let (send, recv) = mpsc::sync_channel::<Job<'scope>>(1);
                scope.spawn(move || {
                    while let Ok(job) = recv.recv() {
                        job();
                    }
                });
                channels.push(send);
            }
        }
        Self { channels, threads }
    }

    pub(super) fn jobs<T, R, S>(
        &self,
        tasks: Vec<T>,
        init: impl Fn(usize) -> R + Send + Sync + 'scope,
        process: impl Fn(&mut R, T) + Send + Sync + 'scope,
        finish: impl Fn(usize, R) -> S + Send + Sync + 'scope,
    ) -> Vec<S>
    where
        T: Send + 'scope,
        R: Send + 'scope,
        S: Send + 'scope,
    {
        if self.channels.is_empty() {
            return crate::input::jobs_finish(tasks, self.threads, init, process, finish);
        }
        let workers = self.threads.min(tasks.len()).max(1);
        let slots: Vec<_> = tasks.into_iter().map(|task| Mutex::new(Some(task))).collect();
        let shared = Arc::new((slots, AtomicUsize::new(0), init, process, finish));
        let (send, recv) = mpsc::sync_channel(workers);
        for (lane, queue) in self.channels.iter().take(workers).enumerate() {
            let shared = shared.clone();
            let send = send.clone();
            queue.send(Box::new(move || {
                let (slots, next, init, process, finish) = &*shared;
                let mut local = init(lane);
                while let Some(slot) = slots.get(next.fetch_add(1, Relaxed)) {
                    process(&mut local, slot.lock().unwrap().take().unwrap());
                }
                send.send((lane, finish(lane, local))).unwrap_or_else(|_| {
                    panic!("worker result receiver was dropped")
                });
            })).unwrap_or_else(|_| panic!("worker input receiver was dropped"));
        }
        drop(send);
        let mut values: Vec<_> = (0..workers).map(|_| recv.recv().unwrap()).collect();
        values.sort_unstable_by_key(|(lane, _)| *lane);
        values.into_iter().map(|(_, value)| value).collect()
    }
}
