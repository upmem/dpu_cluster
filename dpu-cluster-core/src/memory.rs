use dpu::DpuId;
use error::ClusterError;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub struct MemoryTransferEntry<'a> {
    pub offset: u32,
    pub reference: &'a mut [u8]
}

impl <'a> MemoryTransferEntry<'a> {
    pub fn from_u8_slice(offset: u32, reference: &'a mut [u8]) -> Self {
        MemoryTransferEntry { offset, reference }
    }

    pub fn from_u32_slice(offset: u32, reference: &'a mut [u32]) -> Self {
        let v = reference;

        let u8_ref = unsafe {
            std::slice::from_raw_parts_mut(
                v.as_ptr() as *mut u8,
                v.len() * std::mem::size_of::<u32>()
            )
        };

        MemoryTransferEntry { offset, reference: u8_ref }
    }

    pub fn ptr(&mut self) -> *mut u8 {
        self.reference.as_mut_ptr()
    }
}

pub struct MemoryTransferRankEntry<'a>(pub HashMap<DpuId, MemoryTransferEntry<'a>>);
pub struct MemoryTransfer<'a>(pub HashMap<u8, MemoryTransferRankEntry<'a>>);

impl <'a> Default for MemoryTransferRankEntry<'a> {
    fn default() -> Self {
        MemoryTransferRankEntry(HashMap::default())
    }
}

impl <'a> Default for MemoryTransfer<'a> {
    fn default() -> Self {
        MemoryTransfer(HashMap::default())
    }
}

impl <'a> MemoryTransfer<'a> {
    pub fn add(&mut self, dpu: DpuId, entry: MemoryTransferEntry<'a>) {
        let (rank_id, _, _) = dpu.members();

        let rank_transfers = match self.0.entry(rank_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(MemoryTransferRankEntry::default()),
        };

        rank_transfers.0.insert(dpu, entry);
    }
}