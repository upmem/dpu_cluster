use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::ThreadHandle;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub struct InputInitializer<I, IT: Iterator<Item=I> + Send> {
    iterator: Box<IT>,
    sender: Sender<I>,
    shutdown: Arc<Mutex<bool>>
}

impl <I: Send + 'static, IT: Iterator<Item=I> + Send + 'static> InputInitializer<I, IT>
{
    pub fn new(iterator: Box<IT>,
               sender: Sender<I>,
               shutdown: Arc<Mutex<bool>>) -> Self {
        InputInitializer { iterator, sender, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(self) {
        let mut wait: Option<Instant> = None;
        let mut preprocess_time = Duration::new(0, 0);

        for item in self.iterator {
            match wait {
                None => (),
                Some(start_instant) => {
                    preprocess_time = preprocess_time + start_instant.elapsed();
                    wait = None;
                },
            }

            if *self.shutdown.lock().unwrap() {
                println!("INITIALIZER: PREPROCESS: {:?}", preprocess_time);
                return;
            } else {
                self.sender.send(item).unwrap();
            }

            wait.get_or_insert_with(|| Instant::now());
        }

        println!("INITIALIZER: PREPROCESS: {:?}", preprocess_time);
    }
}