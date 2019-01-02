use core::mem;

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

impl InputMemoryTransfer {
    pub fn from_u8_vec(offset: u32, content: Vec<u8>) -> Self {
        InputMemoryTransfer { offset, content }
    }

    pub fn from_u32_vec(offset: u32, mut content: Vec<u32>) -> Self {
        let content = unsafe {
            let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();

            let length = content.len() * ratio;
            let capacity = content.capacity() * ratio;
            let ptr = content.as_mut_ptr() as *mut u8;

            // Don't run the destructor for vec32
            mem::forget(content);

            // Construct new Vec
            Vec::from_raw_parts(ptr, length, capacity)
        };

        InputMemoryTransfer { offset, content }
    }
}