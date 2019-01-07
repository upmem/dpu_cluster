use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::ThreadHandle;
use std::thread;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Event;
use pipeline::monitoring::Process;

pub struct InputInitializer<I, IT: Iterator<Item=I> + Send> {
    iterator: Box<IT>,
    sender: Sender<I>,
    monitoring: EventMonitor,
    shutdown: Arc<Mutex<bool>>
}

impl <I: Send + 'static, IT: Iterator<Item=I> + Send + 'static> InputInitializer<I, IT>
{
    pub fn new(iterator: Box<IT>,
               sender: Sender<I>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Initializer);

        InputInitializer { iterator, sender, monitoring, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(self) {
        let mut monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        for item in self.iterator {
            monitoring.record(Event::NewInput);

            if *self.shutdown.lock().unwrap() {
                break;
            } else {
                self.sender.send(item).unwrap();
            }
        }

        monitoring.record(Event::ProcessEnd);
    }
}