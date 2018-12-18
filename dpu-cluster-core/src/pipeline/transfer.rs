pub struct MemoryTransfers {
    pub inputs: Vec<InputMemoryTransfer>,
    pub output: OutputMemoryTransfer
}

pub struct InputMemoryTransfer {
    pub offset: u32,
    pub content: Vec<u8>
}

pub struct OutputMemoryTransfer {
    pub offset: u32,
    pub length: u32,
}