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

pub struct Plan<'a, T, F: Fn(T) -> MemoryTransfers + Send, IT: Iterator<Item=T> + Send, M: EventMonitor + Clone + Send> {
    base_iterator: Box<IT>,
    program: Option<&'a Program>,
    transfers_fn: Box<F>,
    monitoring: M,
}

impl <'a, T, F: Fn(T) -> MemoryTransfers + Send + 'static, IT: Iterator<Item=T> + Send, M: EventMonitor + Clone + Send> Plan<'a, T, F, IT, M> {
    pub fn running(mut self, program: &'a Program) -> Self {
        self.program = Some(program);
        self
    }
}

impl <'a, T, F, IT, M> Plan<'a, T, F, IT, M>
    where T: Send + 'static,
          IT: Iterator<Item=T> + Send + 'static,
          F: Fn(T) -> MemoryTransfers + Send + 'static,
          M: EventMonitor + Clone + Send + 'static
{
    pub fn new(iter: IT, transfers: F, monitoring: M) -> Self {
        Plan {
            base_iterator: Box::new(iter),
            program: None,
            transfers_fn: Box::new(transfers),
            monitoring
        }
    }

    pub fn execute(self, cluster: Cluster) -> Result<Output, PipelineError> {
        let mut monitoring = self.monitoring;

        let (nr_ranks, nr_slices, nr_dpus) = cluster.topology();

        monitoring.record(Event::Initialization { nr_ranks, nr_slices, nr_dpus });

        if let Some(program) = self.program {
            let nr_instructions = program.iram_sections.iter()
                .fold(0u32, |acc, (_, instructions)| acc + (instructions.len() as u32));
            let nr_data_bytes = program.wram_sections.iter()
                .fold(0u32, |acc, (_, data)| acc + ((data.len() as u32) * 4));

            monitoring.record(Event::LoadingProgramBegin { nr_instructions, nr_data_bytes });
            cluster.driver().load(&View::all(), program)?;
            monitoring.record(Event::LoadingProgramEnd);
        }

        let pipeline = Pipeline::new(self.base_iterator, Arc::new(cluster), self.transfers_fn, monitoring);

        Ok(Output::new(pipeline))
    }
}