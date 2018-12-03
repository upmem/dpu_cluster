use program::Program;
use error::ClusterError;
use memory::MemoryImageCollection;
use view::ViewSelection;
use dpu_sys::DpuRank;
use dpu_sys::DpuRankDescription;
use memory::MemoryImage;
use view::Selection;
use view::DpuId;

#[derive(Debug)]
pub struct Driver {
    ranks: Vec<DpuRank>,
    rank_description: DpuRankDescription,
    current_selection: ViewSelection
}

impl Driver {
    pub fn new(ranks: Vec<DpuRank>, rank_description: DpuRankDescription) -> Self {
        Driver {ranks, rank_description, current_selection: Default::default()}
    }

    pub fn nr_of_dpus(&self) -> usize {
        self.ranks.len() *
            (self.rank_description.topology.nr_of_control_interfaces as usize) *
            (self.rank_description.topology.nr_of_dpus_per_control_interface as usize)
    }

    pub fn load(&self, selection: &ViewSelection, program: &Program) -> Result<(), ClusterError> {
        let ViewSelection(sel) = selection;

        match sel {
            Selection::None => Ok(()),
            Selection::All => self.load_all(program),
            Selection::One(dpu) => self.load_dpu(dpu, program),
            Selection::Only(rank_sel) => unimplemented!(), // todo
        }
    }

    pub fn populate(&self, selection: &ViewSelection, images: &[MemoryImage]) -> Result<(), ClusterError> {
        unimplemented!()
    }

    pub fn run(&self, selection: &ViewSelection) -> Result<(), ClusterError> {
        unimplemented!()
    }

    pub fn dump(&self, selection: &ViewSelection) -> Result<MemoryImageCollection, ClusterError> {
        unimplemented!()
    }

    fn load_all(&self, program: &Program) -> Result<(), ClusterError> {
        for rank in &self.ranks {
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

    fn destructure(&self, dpu: &DpuId) -> (&DpuRank, u8, u8) {
        let DpuId(value) = dpu;
        let member_id = (value & 0xff) as u8;
        let slice_id = ((value >> 8) & 0xFFu32) as u8;
        let rank_id = (value >> 16) as usize;
        // unwrap: DpuId are checked during their creation (when building the View)
        let rank = self.ranks.get(rank_id).unwrap();

        (rank, slice_id, member_id)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        unimplemented!()
    }
}