use program::Program;
use view::View;
use pipeline::transfer::MemoryTransfers;
use pipeline::PipelineError;
use pipeline::output::Output;
use pipeline::pipeline::Pipeline;
use std::sync::Arc;
use cluster::Cluster;
use pipeline::transfer::OutputMemoryTransfer;

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
        if let Some(program) = self.program {
            cluster.driver().load(&View::all(), program)?
        }

        let pipeline = Pipeline::new(self.base_iterator, cluster, self.transfers_fn);

        Ok(Output::new(pipeline))
    }
}

fn no_transfer<T>(_: T) -> MemoryTransfers {
    MemoryTransfers {
        inputs: Vec::default(),
        output: OutputMemoryTransfer { offset: 0, length: 0 }
    }
}