use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::ThreadHandle;
use std::thread;

pub struct InputInitializer<I> {
    iterator: Box<dyn Iterator<Item = I>>,
    sender: Sender<I>,
    shutdown: Arc<Mutex<bool>>
}

impl <I: Send + 'static> InputInitializer<I>
    where Iterator<Item=I>: Send
{
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