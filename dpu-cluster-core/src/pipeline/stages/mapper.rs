use crate::pipeline::stages::Stage;
use crate::pipeline::monitoring::Event;
use crate::pipeline::stages::DpuGroup;
use std::sync::mpsc::Receiver;
use crate::pipeline::transfer::MemoryTransfers;
use std::sync::Mutex;
use std::sync::Arc;
use crate::pipeline::monitoring::EventMonitor;
use crate::pipeline::monitoring::Process;
use std::sync::mpsc::Sender;
use crate::pipeline::transfer::OutputMemoryTransfer;
use crate::pipeline::transfer::InputMemoryTransfer;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use crate::dpu::DpuId;
use std::hash::Hash;
use crate::pipeline::OutputResult;
use std::sync::mpsc::SyncSender;
use crate::pipeline::PipelineError;
use crate::pipeline::GroupId;
use crate::cluster::Cluster;
use crate::memory::MemoryTransfer;

struct BaseMapper<InputItem, InputHandle> {
    groups: Vec<DpuGroup>,
    input_receiver: Receiver<InputItem>,
    group_receiver: Receiver<DpuGroup>,
    transfer_sender: Sender<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(InputHandle, OutputMemoryTransfer)>)>,
    monitoring: EventMonitor,
    // todo: use or remove
    shutdown: Arc<Mutex<bool>>
}

pub struct SimpleMapper<InputItem, InputHandle> {
    base: BaseMapper<InputItem, InputHandle>,
    get_transfers: Box<dyn Fn(InputItem) -> MemoryTransfers<InputHandle> + Send>
}

pub struct PersistentMapper<InputItem, InputHandle, FragmentId, FragmentIterator> {
    base: BaseMapper<InputItem, InputHandle>,
    cluster: Arc<Cluster>,
    get_transfers: Box<dyn Fn(InputItem) -> (FragmentId, MemoryTransfers<InputHandle>) + Send>,
    output_sender: SyncSender<OutputResult<InputHandle>>,
    mapping: Box<FragmentIterator>
}

impl <I, K> SimpleMapper<I, K>
    where I: Send + 'static,
          K: Send + 'static
{
    pub fn new(get_transfers: Box<dyn Fn(I) -> MemoryTransfers<K> + Send>,
               groups: Vec<DpuGroup>,
               input_receiver: Receiver<I>,
               group_receiver: Receiver<DpuGroup>,
               transfer_sender: Sender<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(K, OutputMemoryTransfer)>)>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Mapper);

        SimpleMapper {
            base: BaseMapper { groups, input_receiver, group_receiver, transfer_sender, monitoring, shutdown },
            get_transfers
        }
    }
}

impl <I, K, D, IT> PersistentMapper<I, K, D, IT>
    where I: Send + 'static,
          K: Send + 'static,
          D: Eq + Hash + Send + 'static,
          IT: Iterator<Item=(D, InputMemoryTransfer)>
{
    pub fn new(get_transfers: Box<dyn Fn(I) -> (D, MemoryTransfers<K>) + Send>,
               groups: Vec<DpuGroup>,
               input_receiver: Receiver<I>,
               group_receiver: Receiver<DpuGroup>,
               cluster: Arc<Cluster>,
               transfer_sender: Sender<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(K, OutputMemoryTransfer)>)>,
               output_sender: SyncSender<OutputResult<K>>,
               mapping: Box<IT>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Mapper);

        PersistentMapper {
            base: BaseMapper { groups, input_receiver, group_receiver, transfer_sender, monitoring, shutdown },
            cluster,
            get_transfers,
            output_sender,
            mapping
        }
    }
}

impl <I, K> Stage for SimpleMapper<I, K>
    where I: Send + 'static,
          K: Send + 'static
{
    fn run(mut self) {
        let monitoring = self.base.monitoring;

        monitoring.record(Event::ProcessBegin);

        let mut iterator = self.base.input_receiver.iter();

        while let Some(item) = iterator.next() {
            monitoring.record(Event::GroupSearchBegin);
            let group = fetch_next_group(&mut self.base.groups, &mut self.base.group_receiver);
            let group_id = group.id;
            monitoring.record(Event::GroupSearchEnd(group_id));

            let group_size = group.dpus.len();
            let mut inputs = Vec::with_capacity(group_size);
            let mut outputs = Vec::with_capacity(group_size);

            let transfers = (self.get_transfers)(item);
            inputs.push(transfers.inputs);
            outputs.push((transfers.key, transfers.output));

            while inputs.len() != group_size {
                match iterator.next() {
                    None => break,
                    Some(item) => {
                        let transfers = (self.get_transfers)(item);
                        inputs.push(transfers.inputs);
                        outputs.push((transfers.key, transfers.output));
                    }
                }
            }

            self.base.transfer_sender.send((group, inputs, outputs)).unwrap();
        }

        monitoring.record(Event::ProcessEnd);
    }
}

fn fetch_next_group(groups: &mut Vec<DpuGroup>, group_receiver: &Receiver<DpuGroup>) -> DpuGroup {
    match groups.pop() {
        Some(grp) => grp,
        None => {
            // todo: fix the issue where no group may be sent because all have failed
            let grp = group_receiver.recv().unwrap();

            loop {
                match group_receiver.try_recv() {
                    Ok(other_group) => groups.push(other_group),
                    Err(_) => break,
                }
            }

            grp
        },
    }
}

impl <I, K, D, IT> Stage for PersistentMapper<I, K, D, IT>
    where I: Send + 'static,
          K: Send + 'static,
          D: Eq + Hash + Send + 'static,
          IT: Iterator<Item=(D, InputMemoryTransfer)> + Send + 'static
{
    fn run(mut self) {
        let monitoring = self.base.monitoring;

        monitoring.record(Event::ProcessBegin);

        let mut waiting_inputs: HashMap<GroupId, HashMap<DpuId, Vec<MemoryTransfers<K>>>> = Default::default();
        let mut available_groups: HashMap<GroupId, (DpuGroup, HashMap<DpuId, MemoryTransfers<K>>)> = Default::default();

        let mut mapping: HashMap<D, (DpuId, GroupId)> = Default::default();

        let driver = self.cluster.driver();

        for group in self.base.groups {
            let group_id = group.id;
            let mut transfers = Vec::with_capacity(group.dpus.len());
            let mut used_dpus = Vec::with_capacity(group.dpus.len());

            for dpu in &group.dpus {
                match self.mapping.next() {
                    None => break,
                    Some((fragment_id, fragment_transfer)) => {
                        mapping.insert(fragment_id, (*dpu, group_id));
                        transfers.push((*dpu, fragment_transfer));
                        used_dpus.push(*dpu);
                    },
                }
            }

            if !transfers.is_empty() {
                let mut memory_transfer = MemoryTransfer::default();

                for (dpu, transfer) in transfers.iter_mut() {
                    memory_transfer.add_in_place(*dpu, transfer.offset, transfer.content.as_mut_slice());
                }

                match driver.copy_to_memory(&mut memory_transfer) {
                    Ok(_) => { available_groups.insert(group_id, (DpuGroup { id: group_id, dpus: used_dpus }, HashMap::default())); },
                    Err(_) => unimplemented!(), // todo handle error
                }
            }
        }

        for item in self.base.input_receiver {
            let (fragment_id, transfers) = (self.get_transfers)(item);

            match mapping.get(&fragment_id) {
                None => self.output_sender.send(Err(PipelineError::UnknownFragmentId)).unwrap(),
                Some((dpu_id, group_id)) => {
                    match available_groups.entry(*group_id) {
                        Entry::Occupied(mut group_entry) => {
                            let should_launch = {
                                let (group, dpus) = group_entry.get_mut();

                                let force_launch = match dpus.entry(*dpu_id) {
                                    Entry::Occupied(_) => {
                                        add_waiting_input(&mut waiting_inputs, *group_id, *dpu_id, transfers);
                                        true
                                    },
                                    Entry::Vacant(dpu_entry) => {
                                        dpu_entry.insert(transfers);
                                        false
                                    },
                                };

                                force_launch || is_group_complete(group, dpus)
                            };

                            if should_launch {
                                let (group, dpus) = group_entry.remove();
                                build_and_launch_group(group, dpus, &self.base.transfer_sender);
                            }
                        },
                        Entry::Vacant(_) => {
                            add_waiting_input(&mut waiting_inputs, *group_id, *dpu_id, transfers);
                        },
                    }
                },
            }

            if still_some_waiting_inputs(&waiting_inputs) {
                let new_groups = fetch_available_groups(&self.base.group_receiver);

                for group in new_groups {
                    let group_id = group.id;

                    match waiting_inputs.get_mut(&group_id) {
                        None => {
                            available_groups.insert(group_id, (group, HashMap::default()));
                        },
                        Some(group_entry) => {
                            let first_entry = extract_first_waiting_input(group_entry);

                            if is_group_complete(&group, &first_entry) {
                                build_and_launch_group(group, first_entry, &self.base.transfer_sender);
                            } else {
                                available_groups.insert(group_id, (group, first_entry));
                            }
                        },
                    }
                }
            }
        }

        for (_, (group, dpus)) in available_groups {
            build_and_launch_group(group, dpus, &self.base.transfer_sender);
        }

        while still_some_waiting_inputs(&waiting_inputs) {
            let new_groups = fetch_available_groups(&self.base.group_receiver);

            for group in new_groups {
                let group_id = group.id;

                if let Some(group_entry) = waiting_inputs.get_mut(&group_id) {
                    let first_entry = extract_first_waiting_input(group_entry);
                    build_and_launch_group(group, first_entry, &self.base.transfer_sender);
                }
            }
        }

        monitoring.record(Event::ProcessEnd);
    }
}

fn still_some_waiting_inputs<K>(waiting_inputs: &HashMap<GroupId, HashMap<DpuId, Vec<MemoryTransfers<K>>>>) -> bool {
    waiting_inputs.iter().any(|(_, e)| e.values().any(|v| !v.is_empty()))
}

fn is_group_complete<T>(group: &DpuGroup, entries: &HashMap<DpuId, T>) -> bool {
    group.dpus.len() == entries.len()
}

fn build_and_launch_group<K>(group: DpuGroup, mut dpus: HashMap<DpuId, MemoryTransfers<K>>,
                             transfer_sender: &Sender<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(K, OutputMemoryTransfer)>)>) {
    let group_size = group.dpus.len();
    let mut inputs = Vec::with_capacity(group_size);
    let mut outputs = Vec::with_capacity(group_size);

    for dpu in &group.dpus {
        match dpus.remove(dpu) {
            None => unimplemented!(), // todo sparse group
            Some(transfers) => {
                inputs.push(transfers.inputs);
                outputs.push((transfers.key, transfers.output));
            },
        }
    }


    transfer_sender.send((group, inputs, outputs)).unwrap();
}

fn add_waiting_input<K>(waiting_inputs: &mut HashMap<GroupId, HashMap<DpuId, Vec<MemoryTransfers<K>>>>,
                            group_id: GroupId, dpu_id: DpuId, transfers: MemoryTransfers<K>) {
    let group_entry = match waiting_inputs.entry(group_id) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(HashMap::default()),
    };

    let dpu_entry = match group_entry.entry(dpu_id) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(Vec::default()),
    };

    dpu_entry.push(transfers);
}

fn fetch_available_groups(group_receiver: &Receiver<DpuGroup>) -> Vec<DpuGroup> {
    group_receiver.try_iter().collect()
}

fn extract_first_waiting_input<K>(waiting_inputs: &mut HashMap<DpuId, Vec<MemoryTransfers<K>>>) -> HashMap<DpuId, MemoryTransfers<K>> {
    let mut firsts = HashMap::default();

    for (dpu_id, waiting_transfers) in waiting_inputs.iter_mut() {
        match waiting_transfers.pop() {
            None => {},
            Some(first) => { firsts.insert(*dpu_id, first); },
        }
    }

    firsts
}