use crate::program::Program;
use crate::pipeline::transfer::MemoryTransfers;
use crate::pipeline::GroupPolicy;
use crate::pipeline::output::Output;
use crate::pipeline::PipelineError;
use crate::pipeline::monitoring::EventMonitor;
use crate::cluster::Cluster;
use crate::pipeline::monitoring::Event;
use crate::view::View;
use crate::pipeline::pipeline::Pipeline;
use std::sync::Arc;
use crate::pipeline::monitoring::RecordPolicy;
use crate::pipeline::transfer::InputMemoryTransfer;
use std::hash::Hash;

pub struct Plan<'a, Model, InputIterator> {
    input_iterator: Box<InputIterator>,
    cluster: Option<Cluster>,
    program: Option<&'a Program>,
    group_policy: GroupPolicy,
    monitoring: EventMonitor,
    model: Model
}

impl <'a, InputItem, InputIterator> Plan<'a, (), InputIterator>
    where InputIterator: Iterator<Item=InputItem>
{
    pub fn from<F>(iterator: F) -> Self
        where F: IntoIterator<Item=InputItem, IntoIter=InputIterator>
    {
        Plan {
            input_iterator: Box::new(iterator.into_iter()),
            cluster: None,
            program: None,
            group_policy: GroupPolicy::default(),
            monitoring: EventMonitor::default(),
            model: ()
        }
    }
}

impl <'a, Model, InputIterator> Plan<'a, Model, InputIterator> {
    pub fn running(mut self, program: &'a Program) -> Self {
        self.program = Some(program);
        self
    }

    pub fn driving(mut self, cluster: Cluster) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn grouped_by(mut self, group_policy: GroupPolicy) -> Self {
        self.group_policy = group_policy;
        self
    }

    pub fn monitored_by(mut self, monitor: RecordPolicy) -> Self {
        self.monitoring.set_policy(monitor);
        self
    }

    fn build_init(cluster: &Cluster, monitoring: &EventMonitor, program: Option<&Program>) -> Result<(), PipelineError> {
        let (nr_ranks, nr_slices, nr_dpus) = cluster.topology();

        monitoring.record(Event::Initialization { nr_ranks, nr_slices, nr_dpus });

        if let Some(program) = program {
            let nr_instructions = program.iram_sections.iter()
                .fold(0u32, |acc, (_, instructions)| acc + (instructions.len() as u32));
            let nr_data_bytes = program.wram_sections.iter()
                .fold(0u32, |acc, (_, data)| acc + ((data.len() as u32) * 4));

            monitoring.record(Event::LoadingProgramBegin { nr_instructions, nr_data_bytes });
            cluster.driver().load(&View::all(), program)?;
            monitoring.record(Event::LoadingProgramEnd);
        }

        Ok(())
    }
}

impl <'a, InputIterator> Plan<'a, (), InputIterator> {
    pub fn for_simple_model<InputItem, InputHandle, TransferFn>(self, func: TransferFn) -> Plan<'a, SimpleModel<TransferFn>, InputIterator>
        where TransferFn: Fn(InputItem) -> MemoryTransfers<InputHandle>,
              InputIterator: Iterator<Item=InputItem>
    {
        Plan {
            input_iterator: self.input_iterator,
            cluster: self.cluster,
            program: self.program,
            group_policy: self.group_policy,
            monitoring: self.monitoring,
            model: SimpleModel { input_transfers_fn: Box::new(func) }
        }
    }
}

pub struct SimpleModel<TransferFn> {
    input_transfers_fn: Box<TransferFn>
}

impl <'a, InputItem, InputIterator, InputHandle, TransferFn> Plan<'a, SimpleModel<TransferFn>, InputIterator>
    where InputItem: Send + 'static,
          InputHandle: Send + 'static,
          InputIterator: Iterator<Item=InputItem> + Send + 'static,
          TransferFn: Fn(InputItem) -> MemoryTransfers<InputHandle> + Send + 'static
{
    pub fn build(self) -> Result<Output<InputHandle>, PipelineError>  {
        let cluster = self.cluster.ok_or_else(|| PipelineError::UndefinedCluster)?;

        Self::build_init(&cluster, &self.monitoring, self.program)?;

        let pipeline = Pipeline::simple(self.input_iterator, Arc::new(cluster),
                                        self.model.input_transfers_fn, self.group_policy, self.monitoring);

        Ok(Output::new(pipeline))
    }
}

pub struct PersistentModel<TransferFn, PersistentIterator> {
    persistent_iterator: Box<PersistentIterator>,
    input_transfers_fn: Box<TransferFn>,
}

impl <'a, InputIterator> Plan<'a, (), InputIterator> {
    pub fn for_persistent_model<InputItem, InputHandle, TransferFn, PersistentHandle, PersistentIterator, IT>(self, func: TransferFn, iterator: IT)
        -> Plan<'a, PersistentModel<TransferFn, PersistentIterator>, InputIterator>
        where TransferFn: Fn(InputItem) -> (PersistentHandle, MemoryTransfers<InputHandle>),
              InputIterator: Iterator<Item=InputItem>,
              PersistentIterator: Iterator<Item=(PersistentHandle, InputMemoryTransfer)>,
              IT: IntoIterator<Item=(PersistentHandle, InputMemoryTransfer), IntoIter=PersistentIterator>
    {
        Plan {
            input_iterator: self.input_iterator,
            cluster: self.cluster,
            program: self.program,
            group_policy: self.group_policy,
            monitoring: self.monitoring,
            model: PersistentModel { input_transfers_fn: Box::new(func), persistent_iterator: Box::new(iterator.into_iter()) }
        }
    }
}

impl <'a, InputItem, InputIterator, InputHandle, TransferFn, PersistentHandle, PersistentIterator> Plan<'a, PersistentModel<TransferFn, PersistentIterator>, InputIterator>
    where InputItem: Send + 'static,
          InputHandle: Send + 'static,
          InputIterator: Iterator<Item=InputItem> + Send + 'static,
          TransferFn: Fn(InputItem) -> (PersistentHandle, MemoryTransfers<InputHandle>) + Send + 'static,
          PersistentHandle: Eq + Hash + Send + 'static,
          PersistentIterator: Iterator<Item=(PersistentHandle, InputMemoryTransfer)> + Send + 'static
{
    pub fn build(self) -> Result<Output<InputHandle>, PipelineError>  {
        let cluster = self.cluster.ok_or_else(|| PipelineError::UndefinedCluster)?;

        Self::build_init(&cluster, &self.monitoring, self.program)?;

        let pipeline = Pipeline::persistent(self.input_iterator, Arc::new(cluster),
                                        self.model.input_transfers_fn, self.model.persistent_iterator, self.group_policy, self.monitoring);

        Ok(Output::new(pipeline))
    }
}