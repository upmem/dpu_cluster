use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;

use dpu::DpuId;
use dpu_sys::DpuRank;
use dpu_sys::DpuRankDescription;
use dpu_sys::DpuRankTransferMatrix;
use error::ClusterError;
use memory::Location;
use memory::MemoryImage;
use memory_utilities;
use memory_utilities::MemoryImageCollection;
use program::Program;
use view::FastSelection;
use view::Selection;
use view::View;
use dpu_sys::DpuError;

#[derive(Debug)]
pub struct Driver {
    rank_handler: Arc<RankHandler>,
    rank_description: DpuRankDescription,
    watcher: WatcherControl
}

#[derive(Debug)]
struct WatcherControl {
    sender: Sender<ClusterMessage>,
    handle: Option<JoinHandle<()>>
}

#[derive(Debug)]
struct RankHandler {
    ranks: Vec<DpuRank>,
    state: Mutex<ClusterState>
}

struct Watcher {
    receiver: Receiver<ClusterMessage>,
    rank_handler: Arc<RankHandler>,
}

#[derive(Debug, Clone)]
pub enum DpuState {
    Idle,
    Running,
    Fault
}

pub enum FaultCause {
    Breakpoint,
    Memory,
    Dma
}

pub struct FaultInformation {
    dpu: DpuId,
    thread: u8,
    cause: FaultCause
}

pub enum RunStatus {
    Idle,
    Running,
    Fault(Vec<FaultInformation>)
}

impl Default for RunStatus {
    fn default() -> Self {
        RunStatus::Idle
    }
}

#[derive(Debug)]
pub struct ClusterState {
    run_bitfields: Vec<Vec<u32>>,
    fault_bitfields: Vec<Vec<u32>>,
    internal_error: Option<DpuError>
}

enum ClusterMessage {
    ToggleActivity,
    Shutdown
}

const BOOTSTRAP_THREAD: u8 = 0;

impl Default for DpuState {
    fn default() -> Self {
        DpuState::Idle
    }
}

impl Driver {
    pub fn new(ranks: Vec<DpuRank>, rank_description: DpuRankDescription) -> Self {
        let run_bitfields = Driver::create_cluster_bitfield(&ranks, &rank_description);
        let fault_bitfields = run_bitfields.clone();
        let state = Mutex::new(ClusterState { run_bitfields, fault_bitfields, internal_error: Option::default() });
        let rank_handler = Arc::new(RankHandler { ranks, state });
        let watcher = Watcher::launch(Arc::clone(&rank_handler));

        Driver { rank_handler, rank_description, watcher }
    }

    fn create_cluster_bitfield(ranks: &[DpuRank], rank_description: &DpuRankDescription) -> Vec<Vec<u32>> {
        let nr_of_ranks = ranks.len();
        let nr_of_control_interfaces_per_rank = rank_description.topology.nr_of_control_interfaces as usize;
        let mut bitfield = Vec::with_capacity(nr_of_control_interfaces_per_rank);
        bitfield.resize(nr_of_control_interfaces_per_rank, 0);

        let mut run_bitfields = Vec::with_capacity(nr_of_ranks);
        run_bitfields.resize(nr_of_ranks, bitfield);
        run_bitfields
    }

    pub fn nr_of_dpus(&self) -> usize {
        self.rank_handler.ranks.len() *
            (self.rank_description.topology.nr_of_control_interfaces as usize) *
            (self.rank_description.topology.nr_of_dpus_per_control_interface as usize)
    }

    pub fn load(&self, view: &View, program: &Program) -> Result<(), ClusterError> {
        let View(selection) = view;

        match selection {
            FastSelection::Fast(dpu) => self.load_dpu(dpu, program),
            FastSelection::Normal(Selection::All) => self.load_all(program),
            FastSelection::Normal(Selection::Some(ranks)) => unimplemented!(), // todo
            FastSelection::Normal(Selection::None) => Ok(()),
        }
    }

    pub fn copy_to_memory(&self, data: HashMap<Location, &MemoryImage>) -> Result<(), ClusterError> {
        let transfers = self.group_memory_transfers_per_rank(data);

        for (rank, rank_transfers) in transfers {
            let matrix = self.create_transfer_matrix_for(rank, rank_transfers)?;
            rank.copy_to_mrams(&matrix);
        }

        Ok(())
    }

    pub fn copy_from_memory(&self, data: HashMap<Location, &MemoryImage>) -> Result<(), ClusterError> {
        let transfers = self.group_memory_transfers_per_rank(data);

        for (rank, rank_transfers) in transfers {
            let matrix = self.create_transfer_matrix_for(rank, rank_transfers)?;
            rank.copy_from_mrams(&matrix);
        }

        Ok(())
    }

    pub fn run(&self, view: &View) -> Result<RunStatus, ClusterError> {
        self.boot(view)?;

        loop {
            match self.fetch_status(view)? {
                RunStatus::Running => (),
                finished => return Ok(finished),
            }
        }
    }

    pub fn boot(&self, view: &View) -> Result<(), ClusterError> {
        let View(selection) = view;

        match selection {
            FastSelection::Fast(dpu) => self.boot_dpu(dpu),
            FastSelection::Normal(Selection::All) => self.boot_all(),
            FastSelection::Normal(Selection::Some(ranks)) => unimplemented!(), // todo
            FastSelection::Normal(Selection::None) => Ok(()),
        }
    }

    pub fn fetch_status(&self, view: &View) -> Result<RunStatus, ClusterError> {
        let View(selection) = view;

        match selection {
            FastSelection::Fast(dpu) => self.fetch_dpu_status(dpu),
            FastSelection::Normal(Selection::All) => self.fetch_all_status(),
            FastSelection::Normal(Selection::Some(ranks)) => unimplemented!(), // todo
            FastSelection::Normal(Selection::None) => Ok(RunStatus::default()),
        }
    }

    fn load_all(&self, program: &Program) -> Result<(), ClusterError> {
        for rank in &self.rank_handler.ranks {
            self.load_rank(rank, program)?;
        }

        Ok(())
    }

    fn load_rank(&self, rank: &DpuRank, program: &Program) -> Result<(), ClusterError> {
        for (offset, instructions) in &program.iram_sections {
            rank.copy_to_irams(instructions.as_ptr(), instructions.len() as u16, *offset)?;
        }
        for (offset, data) in &program.wram_sections {
            rank.copy_to_wrams(data.as_ptr(), data.len() as u32, *offset)?;
        }

        Ok(())
    }

    fn load_dpu(&self, dpu: &DpuId, program: &Program) -> Result<(), ClusterError> {
        let (rank, slice, member) = self.destructure(dpu);

        for (offset, instructions) in &program.iram_sections {
            rank.copy_to_iram(slice, member, instructions.as_ptr(), instructions.len() as u16, *offset)?;
        }
        for (offset, data) in &program.wram_sections {
            rank.copy_to_wram(slice, member, data.as_ptr(), data.len() as u32, *offset)?;
        }

        Ok(())
    }

    fn boot_all(&self) -> Result<(), ClusterError> {
        let nr_of_slices = self.rank_description.topology.nr_of_control_interfaces as usize;
        let mut was_running = Vec::with_capacity(nr_of_slices);
        was_running.resize(nr_of_slices, 0);

        for rank in &self.rank_handler.ranks {
            self.boot_rank(rank, &mut was_running)?;
        }

        Ok(())
    }

    fn boot_rank(&self, rank: &DpuRank, was_running: &mut [u32]) -> Result<(), ClusterError> {
        rank.launch_thread_on_all(BOOTSTRAP_THREAD, false, was_running.as_mut_ptr())?;

        Ok(())
    }

    fn boot_dpu(&self, dpu: &DpuId) -> Result<(), ClusterError> {
        let mut was_running = false;
        let (rank, slice, member) = self.destructure(dpu);

        rank.launch_thread_on_dpu(slice, member, BOOTSTRAP_THREAD, false, &mut was_running)?;

        Ok(())
    }

    fn fetch_all_status(&self) -> Result<RunStatus, ClusterError> {
        let mut running = false;
        let mut fault = false;
        let mut faults = Vec::default();
        // unwrap: no owner of the mutex should panic
        let state = self.rank_handler.state.lock().unwrap();
        let nr_of_ranks = state.run_bitfields.len();
        let nr_of_control_interfaces_per_rank = self.rank_description.topology.nr_of_control_interfaces as usize;

        if let Some(ref err) = state.internal_error {
            Err(err.clone().into())
        } else {
            for rank_id in 0..nr_of_ranks {
                for slice_id in 0..nr_of_control_interfaces_per_rank {
                    if state.run_bitfields[rank_id][slice_id] != 0 {
                        running = true;
                    }
                    if state.fault_bitfields[rank_id][slice_id] != 0 {
                        fault = true;
                        unimplemented!(); // todo
                    }
                }
            }

            if !running {
                Ok(RunStatus::Idle)
            } else if !fault {
                Ok(RunStatus::Running)
            } else {
                Ok(RunStatus::Fault(faults))
            }
        }
    }

    fn fetch_dpu_status(&self, dpu: &DpuId) -> Result<RunStatus, ClusterError> {
        let (rank, slice, member) = dpu.members();
        let mask = 1 << (member as u32);
        let rank_id = rank as usize;
        let slice_id = slice as usize;
        // unwrap: no owner of the mutex should panic
        let state = self.rank_handler.state.lock().unwrap();

        if let Some(ref err) = state.internal_error {
            Err(err.clone().into())
        } else {
            let slice_run = state.run_bitfields[rank_id][slice_id];
            let slice_fault = state.run_bitfields[rank_id][slice_id];

            if (slice_run & mask) == 0 {
                Ok(RunStatus::Idle)
            } else if (slice_fault & mask) == 0 {
                Ok(RunStatus::Running)
            } else {
                unimplemented!() // todo
            }
        }
    }

    fn group_memory_transfers_per_rank<'a>(&self, data: HashMap<Location, &'a MemoryImage>) -> HashMap<&DpuRank, HashMap<Location, &'a MemoryImage>> {
        let mut transfers_per_rank = HashMap::default();

        for (location, image) in data {
            let (rank, _, _) = self.destructure(&location.dpu);

            let transfers = match transfers_per_rank.entry(rank) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(HashMap::default()),
            };

            transfers.insert(location, image);
        }

        transfers_per_rank
    }

    fn create_transfer_matrix_for<'a>(&self, rank: &'a DpuRank, mut data: HashMap<Location, &MemoryImage>) -> Result<DpuRankTransferMatrix<'a>, ClusterError> {
        let matrix = DpuRankTransferMatrix::allocate_for(rank)?;

        for (location, image) in data.iter_mut() {
            let (_, slice, member) = location.dpu.members();
            let mut content = image.content()?;

            matrix.add_dpu(slice, member, content.as_mut_ptr(), content.len() as u32, location.offset, memory_utilities::PRIMARY_MRAM);
        }

        Ok(matrix)
    }

    fn destructure(&self, dpu: &DpuId) -> (&DpuRank, u8, u8) {
        let (rank_id, slice_id, member_id) = dpu.members();
        let rank = self.rank_handler.get_rank(rank_id);

        (rank, slice_id, member_id)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // unwrap: handle is always Some(_), until this unique call to drop
        let handle = self.watcher.handle.take().unwrap();

        self.watcher.sender.send(ClusterMessage::Shutdown);
        handle.join();
    }
}

impl Watcher {
    fn launch(rank_handler: Arc<RankHandler>) -> WatcherControl {
        let (sender, receiver) = mpsc::channel();
        let mut watcher = Watcher::new(receiver, rank_handler);
        let handle = thread::spawn(move || watcher.run());

        WatcherControl { sender, handle: Some(handle) }
    }

    fn new(receiver: Receiver<ClusterMessage>, rank_handler: Arc<RankHandler>) -> Watcher {
        Watcher { receiver, rank_handler }
    }

    fn run(&mut self) -> () {
        loop {
            match self.receiver.try_recv() {
                Ok(ClusterMessage::ToggleActivity) =>
                    match self.receiver.recv() {
                        Ok(ClusterMessage::ToggleActivity) => (),
                        Ok(ClusterMessage::Shutdown) => return,
                        Err(err) => return,
                    },
                Ok(ClusterMessage::Shutdown) => return,
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    return;
                },
            };

            match self.update_cluster_state() {
                Err(err) => {
                    // unwrap: no owner of the mutex should panic
                    self.rank_handler.state.lock().unwrap().internal_error = Some(err);
                    return;
                },
                Ok(_) => ()
            }
        }
    }

    fn update_cluster_state(&mut self) -> Result<(), DpuError> {
        // unwrap: no owner of the mutex should panic
        let mut state = self.rank_handler.state.lock().unwrap();

        for (rank_idx, rank) in self.rank_handler.ranks.iter().enumerate() {
            rank.poll_all(state.run_bitfields[rank_idx].as_mut_ptr(), state.fault_bitfields[rank_idx].as_mut_ptr())?;
        }

        Ok(())
    }
}

impl RankHandler {
    fn get_rank(&self, rank_id: u8) -> &DpuRank {
        // unwrap: DpuId are checked during their creation
        self.ranks.get(rank_id as usize).unwrap()
    }
}