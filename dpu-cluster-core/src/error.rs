use dpu_sys::DpuError;
use dpu_elf_loader::LoaderError;
use driver::DpuState;

#[derive(Debug)]
pub enum ClusterError {
    NotEnoughResources {expected: u32, found: u32 },
    IncorrectMemoryImageSize(usize),
    InvalidCommandInState {command: String, state: DpuState},
    LoadingError(LoaderError),
    LowLevelError(DpuError),
    IOError(std::io::Error)
}

impl From<LoaderError> for ClusterError {
    fn from(err: LoaderError) -> Self {
        ClusterError::LoadingError(err)
    }
}

impl From<DpuError> for ClusterError {
    fn from(err: DpuError) -> Self {
        ClusterError::LowLevelError(err)
    }
}

impl From<std::io::Error> for ClusterError {
    fn from(err: std::io::Error) -> Self {
        ClusterError::IOError(err)
    }
}