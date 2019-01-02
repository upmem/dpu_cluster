use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::ThreadHandle;
use std::thread;

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
        for item in self.iterator {
            if *self.shutdown.lock().unwrap() {
                return;
            } else {
                self.sender.send(item).unwrap();
            }
        }
    }
}