use error::ClusterError;
use std::fs::File;
use std::io::Read;
use std::path::Path;

const MRAM_SIZE: usize = 1 << 26;

pub type MemoryImageCollection = Vec<MemoryImage>;
pub type MemoryImage = [u8; MRAM_SIZE];

pub fn from_directory<P: AsRef<Path>>(path: P) -> Result<MemoryImageCollection, ClusterError> {
    let mut images = Vec::default();

    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if !path.is_dir() {
            let image = from_file(path)?;
            images.push(image);
        }
    }

    Ok(images)
}

pub fn from_file<P: AsRef<Path>>(path: P) -> Result<MemoryImage, ClusterError> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();

    if size != (MRAM_SIZE as u64) {
        return Err(ClusterError::IncorrectMemoryImageSize(size as usize))
    }

    let mut buffer: [u8; MRAM_SIZE] = [0; MRAM_SIZE];
    file.read(&mut buffer)?;

    Ok(buffer)
}
