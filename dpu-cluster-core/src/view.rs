use memory::MemoryImageCollection;
use error::ClusterError;
use program::Program;
use driver::Driver;
use memory::MemoryImage;

#[derive(Debug)]
pub enum Selection<T> {
    All,
    None,
    One(DpuId),
    Only(Vec<T>)
}

impl <T> Default for Selection<T> {
    fn default() -> Self {
        Selection::None
    }
}

#[derive(Debug, Default)]
pub struct ViewSelection(pub Selection<Selection<Selection<DpuId>>>);

impl ViewSelection {
    pub fn all() -> Self {
        ViewSelection(Selection::All)
    }
}

#[derive(Debug)]
pub struct DpuId(pub u32);

pub trait View {
    fn selection(&self) -> &ViewSelection;
    fn driver(&self) -> &Driver;

    fn nr_of_dpus(&self) -> usize {
        self.driver().nr_of_dpus()
    }

    fn load(&self, program: &Program) -> Result<(), ClusterError> {
        self.driver().load(self.selection(), program)
    }

    fn populate(&self, images: &[MemoryImage]) -> Result<(), ClusterError> {
        self.driver().populate(self.selection(), images)
    }

    fn run(&self) -> Result<(), ClusterError> {
        self.driver().run(self.selection())
    }

    fn dump(&self) -> Result<MemoryImageCollection, ClusterError> {
        self.driver().dump(self.selection())
    }
}