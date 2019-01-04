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
use program::Program;
use view::FastSelection;
use view::Selection;
use view::View;
use dpu_sys::DpuError;
use dpu_sys::DpuDebugContext;
use memory::MemoryTransfer;
use memory::MemoryTransferRankEntry;
use std::time::Duration;

#[derive(Debug)]
pub struct Driver {
    rank_handler: RankHandler,
    pub nr_of_ranks: u8,
    pub rank_description: DpuRankDescription
}

unsafe impl Sync for Driver {}

#[derive(Debug)]
struct RankHandler {
    ranks: Vec<DpuRank>
}

#[derive(Clone)]
pub enum FaultCause {
    Breakpoint,
    Memory,
    Dma
}

pub struct FaultInformation {
    dpu: DpuId,
    context: DpuDebugContext
}

pub enum RunStatus {
    Idle,
    Running,
    Fault(Vec<DpuId>)
}

impl Default for RunStatus {
    fn default() -> Self {
        RunStatus::Idle
    }
}

const BOOTSTRAP_THREAD: u8 = 0;
const PRIMARY_MRAM: u32 = 0;

trait FromRankId<'a> {
    fn from_rank_id(rank_id: u8, handler: &'a RankHandler) -> Self;
}

impl <'a> FromRankId<'a> for u8 {
    fn from_rank_id(rank_id: u8, _: &'a RankHandler) -> Self {
        rank_id
    }
}

impl <'a> FromRankId<'a> for &'a DpuRank {
    fn from_rank_id(rank_id: u8, handler: &'a RankHandler) -> Self {
        handler.get_rank(rank_id)
    }
}

pub trait Mergeable {
    fn merge_with(&self, other: &Self) -> Self;
}

impl Mergeable for () {
    fn merge_with(&self, _: &Self) -> Self {
        ()
    }
}

impl Mergeable for RunStatus {
    fn merge_with(&self, other: &Self) -> Self {
        match (self, other) {
            (RunStatus::Idle, RunStatus::Idle) => RunStatus::Idle,
            (RunStatus::Idle, RunStatus::Running) => RunStatus::Running,
            (RunStatus::Idle, RunStatus::Fault(other_faults)) => RunStatus::Fault(other_faults.to_vec()),
            (RunStatus::Running, RunStatus::Idle) => RunStatus::Running,
            (RunStatus::Running, RunStatus::Running) => RunStatus::Running,
            (RunStatus::Running, RunStatus::Fault(other_faults)) => RunStatus::Fault(other_faults.to_vec()),
            (RunStatus::Fault(faults), RunStatus::Idle) => RunStatus::Fault(faults.to_vec()),
            (RunStatus::Fault(faults), RunStatus::Running) => RunStatus::Fault(faults.to_vec()),
            (RunStatus::Fault(faults), RunStatus::Fault(other_faults)) => {
                let mut all_faults = faults.to_vec();
                all_faults.append(&mut other_faults.to_vec());
                RunStatus::Fault(all_faults)
            },
        }
    }
}

impl Driver {
    pub fn new(ranks: Vec<DpuRank>, rank_description: DpuRankDescription) -> Self {
        let nr_of_ranks = ranks.len() as u8;
        let rank_handler = RankHandler { ranks };

        Driver { rank_handler, nr_of_ranks, rank_description }
    }

    pub fn nr_of_dpus(&self) -> usize {
        self.rank_handler.ranks.len() *
            (self.rank_description.topology.nr_of_control_interfaces as usize) *
            (self.rank_description.topology.nr_of_dpus_per_control_interface as usize)
    }

    pub fn load(&self, view: &View, program: &Program) -> Result<(), ClusterError> {
        self.dispatch(view,
                      |dpu| self.load_dpu(dpu, program),
                      |rank| self.load_rank(rank, program),
                      || self.load_all(program))
    }

    pub fn boot(&self, view: &View) -> Result<(), ClusterError> {
        self.dispatch(view,
                      |dpu| self.boot_dpu(dpu),
                      |rank| self.boot_rank(rank),
                      || self.boot_all())
    }

    pub fn fetch_status(&self, view: &View) -> Result<RunStatus, ClusterError> {
        self.dispatch(view,
                      |dpu| self.fetch_dpu_status(dpu),
                      |rank| self.fetch_rank_status(rank),
                      || self.fetch_all_status())
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

    pub fn copy_to_memory(&self, data: &mut MemoryTransfer) -> Result<(), ClusterError> {
        for (rank_id, rank_transfers) in data.0.iter_mut() {
            let rank= self.rank_handler.get_rank(*rank_id);
            let matrix = self.create_transfer_matrix_for(rank, rank_transfers)?;
            rank.copy_to_mrams(&matrix)?;
        }

        Ok(())
    }

    pub fn copy_from_memory(&self, data: &mut MemoryTransfer) -> Result<(), ClusterError> {
        for (rank_id, rank_transfers) in data.0.iter_mut() {
            let rank= self.rank_handler.get_rank(*rank_id);
            let matrix = self.create_transfer_matrix_for(rank, rank_transfers)?;
            rank.copy_from_mrams(&matrix)?;
        }

        Ok(())
    }

    pub fn fetch_dpu_fault_context(&self, dpu: &DpuId) -> Result<FaultInformation, ClusterError> {
        let (rank, slice_id, member) = self.destructure(dpu);
        let mut context =
            DpuDebugContext::new(self.rank_description.info.nr_of_threads,
                                 self.rank_description.info.nr_of_work_registers_per_thread,
                                 self.rank_description.info.nr_of_atomic_bits);
        rank.initialize_fault_process_for_dpu(slice_id, member, &mut context)?;
        Ok(FaultInformation { dpu: dpu.clone(), context })
    }

    fn dispatch<'a, T, FnRankArg, FnDpu, FnRank, FnAll>(&'a self, view: &View, for_dpu: FnDpu, for_rank: FnRank, for_all: FnAll) -> Result<T, ClusterError>
        where T: Default + Mergeable,
              FnRankArg: FromRankId<'a>,
              FnDpu: Fn(&DpuId) -> Result<T, ClusterError>,
              FnRank: Fn(FnRankArg) -> Result<T, ClusterError>,
              FnAll: FnOnce() -> Result<T, ClusterError>
    {
        let View(selection) = view;

        match selection {
            FastSelection::Fast(dpu) => for_dpu(dpu),
            FastSelection::Normal(Selection::All) => for_all(),
            FastSelection::Normal(Selection::None) => Ok(T::default()),
            FastSelection::Normal(Selection::Some(ranks)) => self.dispatch_for_some_ranks(ranks, for_dpu, for_rank),
        }
    }

    fn dispatch_for_some_ranks<'a, T, FnRankArg, FnDpu, FnRank>(&'a self, ranks: &[Selection<Selection<DpuId>>], for_dpu: FnDpu, for_rank: FnRank) -> Result<T, ClusterError>
        where T: Default + Mergeable,
              FnRankArg: FromRankId<'a>,
              FnDpu: Fn(&DpuId) -> Result<T, ClusterError>,
              FnRank: Fn(FnRankArg) -> Result<T, ClusterError>
    {
        let mut result = T::default();
        for (rank_id, rank_selection) in ranks.iter().enumerate() {
            match rank_selection {
                Selection::All => {
                    let rank_result = for_rank(FnRankArg::from_rank_id(rank_id as u8, &self.rank_handler))?;
                    result = result.merge_with(&rank_result);
                },
                Selection::None => (),
                Selection::Some(control_interfaces) => {
                    // todo: we can be more efficient when the C Interface changes
                    let mut dpus = Vec::default();
                    let nr_of_dpus_per_control_interface = self.rank_description.topology.nr_of_dpus_per_control_interface;

                    for (slice_id, slice_selection) in control_interfaces.iter().enumerate() {
                        match slice_selection {
                            Selection::All => {
                                for member_id in 0..nr_of_dpus_per_control_interface {
                                    dpus.push(DpuId::new(rank_id as u8, slice_id as u8, member_id));
                                }
                            },
                            Selection::None => (),
                            Selection::Some(dpu_ids) => dpus.append(&mut dpu_ids.to_vec()),
                        }
                    }

                    for dpu in dpus {
                        let dpu_result = for_dpu(&dpu)?;
                        result = result.merge_with(&dpu_result);
                    }
                },
            }
        }

        Ok(result)
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
        for rank in &self.rank_handler.ranks {
            self.boot_rank(rank)?;
        }

        Ok(())
    }

    fn boot_rank(&self, rank: &DpuRank) -> Result<(), ClusterError> {
        let nr_of_slices = self.rank_description.topology.nr_of_control_interfaces as usize;
        let mut was_running = Vec::with_capacity(nr_of_slices);
        was_running.resize(nr_of_slices, 0);

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
        let nr_of_ranks = self.nr_of_ranks;
        let mut status = RunStatus::default();

        for rank_id in 0..nr_of_ranks {
            let rank_status = self.fetch_rank_status(rank_id)?;
            status = status.merge_with(&rank_status);
        }

        Ok(status)
    }

    fn fetch_rank_status(&self, rank_id: u8) -> Result<RunStatus, ClusterError> {
        let mut running = false;
        let mut fault = false;
        let mut faults = Vec::default();
        let nr_of_control_interfaces_per_rank = self.rank_description.topology.nr_of_control_interfaces as usize;
        let nr_of_dpus_per_control_interface = self.rank_description.topology.nr_of_dpus_per_control_interface;

        let rank = self.rank_handler.get_rank(rank_id);
        let mut run_bitfields = vec![0; nr_of_control_interfaces_per_rank];
        let mut fault_bitfields = vec![0; nr_of_control_interfaces_per_rank];

        rank.poll_all(run_bitfields.as_mut_ptr(), fault_bitfields.as_mut_ptr())?;

        for slice_id in 0..nr_of_control_interfaces_per_rank {
            if run_bitfields[slice_id] == 0 {
                continue;
            }

            running = true;
            let fault_bitfield = fault_bitfields[slice_id];
            if fault_bitfield != 0 {
                fault = true;

                for member in 0..nr_of_dpus_per_control_interface {
                    if (fault_bitfield & (1 << (member as u32))) != 0 {
                        faults.push(DpuId::new(rank_id, slice_id as u8, member));
                    }
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

    fn fetch_dpu_status(&self, dpu: &DpuId) -> Result<RunStatus, ClusterError> {
        let (rank, slice, member) = self.destructure(dpu);

        let mut running = false;
        let mut fault = false;

        rank.poll_dpu(slice, member, &mut running, &mut fault)?;

        if !running {
            Ok(RunStatus::Idle)
        } else if !fault {
            Ok(RunStatus::Running)
        } else {
            Ok(RunStatus::Fault(vec![dpu.clone()]))
        }
    }

    fn create_transfer_matrix_for<'a>(&self, rank: &'a DpuRank, data: &mut MemoryTransferRankEntry) -> Result<DpuRankTransferMatrix<'a>, ClusterError> {
        let matrix = DpuRankTransferMatrix::allocate_for(rank)?;

        for (dpu, image) in data.0.iter_mut() {
            let (_, slice, member) = dpu.members();
            let offset = image.offset;
            let length = image.reference.len() as u32;

            matrix.add_dpu(slice, member, image.ptr(), length, offset, PRIMARY_MRAM);
        }

        Ok(matrix)
    }

    fn destructure(&self, dpu: &DpuId) -> (&DpuRank, u8, u8) {
        let (rank_id, slice_id, member_id) = dpu.members();
        let rank = self.rank_handler.get_rank(rank_id);

        (rank, slice_id, member_id)
    }
}

impl RankHandler {
    fn get_rank(&self, rank_id: u8) -> &DpuRank {
        // unwrap: DpuId are checked during their creation
        self.ranks.get(rank_id as usize).unwrap()
    }
}