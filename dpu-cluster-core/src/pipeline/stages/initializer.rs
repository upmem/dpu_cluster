use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Event;
use pipeline::monitoring::Process;
use pipeline::stages::Stage;

pub struct InputInitializer<InputItem, InputIterator> {
    iterator: Box<InputIterator>,
    sender: SyncSender<InputItem>,
    monitoring: EventMonitor,
    shutdown: Arc<Mutex<bool>>
}

impl <InputItem, InputIterator> InputInitializer<InputItem, InputIterator>
    where InputItem: Send + 'static,
          InputIterator: Iterator<Item=InputItem> + Send + 'static
{
    pub fn new(iterator: Box<InputIterator>,
               sender: SyncSender<InputItem>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Initializer);

        InputInitializer { iterator, sender, monitoring, shutdown }
    }
}

impl <InputItem, InputIterator> Stage for InputInitializer<InputItem, InputIterator>
    where InputItem: Send + 'static,
          InputIterator: Iterator<Item=InputItem> + Send + 'static
{
    fn run(self) {
        let monitoring = self.monitoring;

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