use program::Program;
use view::View;
use pipeline::transfer::MemoryTransfers;
use pipeline::PipelineError;
use pipeline::output::Output;
use pipeline::pipeline::Pipeline;
use std::sync::Arc;
use cluster::Cluster;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Event;

pub struct Plan<'a, T, F: Fn(T) -> MemoryTransfers + Send, IT: Iterator<Item=T> + Send> {
    base_iterator: Box<IT>,
    program: Option<&'a Program>,
    transfers_fn: Box<F>,
}

impl <'a, T, F: Fn(T) -> MemoryTransfers + Send + 'static, IT: Iterator<Item=T> + Send> Plan<'a, T, F, IT> {
    pub fn running(mut self, program: &'a Program) -> Self {
        self.program = Some(program);
        self
    }
}

impl <'a, T, F, IT> Plan<'a, T, F, IT>
    where T: Send + 'static,
          IT: Iterator<Item=T> + Send + 'static,
          F: Fn(T) -> MemoryTransfers + Send + 'static
{
    pub fn new(iter: IT, transfers: F) -> Self {
        Plan {
            base_iterator: Box::new(iter),
            program: None,
            transfers_fn: Box::new(transfers)
        }
    }

    pub fn execute(self, cluster: Arc<Cluster>) -> Result<Output, PipelineError> {
        let mut monitoring = EventMonitor::new();

        if let Some(program) = self.program {
            monitoring.record(Event::LoadingProgramBegin);
            cluster.driver().load(&View::all(), program)?;
            monitoring.record(Event::LoadingProgramEnd);
        }

        let pipeline = Pipeline::new(self.base_iterator, cluster, self.transfers_fn, monitoring);

        Ok(Output::new(pipeline))
    }
}