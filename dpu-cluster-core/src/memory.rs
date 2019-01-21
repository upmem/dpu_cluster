use crate::dpu::DpuId;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub struct MemoryTransferEntry<'a> {
    pub offset: u32,
    pub reference: &'a mut [u8]
}

pub struct MemoryTransferEntryReference<'a>(&'a mut [u8]);

impl <'a> MemoryTransferEntry<'a> {
    pub fn ptr(&mut self) -> *mut u8 {
        self.reference.as_mut_ptr()
    }
}

impl <'a> From<&'a mut [u8]> for MemoryTransferEntryReference<'a> {
    fn from(v: &'a mut [u8]) -> Self {
        MemoryTransferEntryReference(v)
    }
}

impl <'a> From<&'a mut [u32]> for MemoryTransferEntryReference<'a> {
    fn from(v: &'a mut [u32]) -> Self {
        let u8_ref = unsafe {
            std::slice::from_raw_parts_mut(
                v.as_ptr() as *mut u8,
                v.len() * std::mem::size_of::<u32>()
            )
        };

        MemoryTransferEntryReference(u8_ref)
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
    pub fn add<I>(mut self, dpu: DpuId, offset: u32, slice: I) -> Self
        where I: Into<MemoryTransferEntryReference<'a>>
    {
        self.add_entry(dpu, MemoryTransferEntry {offset, reference: slice.into().0});
        self
    }

    pub fn add_in_place<I>(&mut self, dpu: DpuId, offset: u32, slice: I)
        where I: Into<MemoryTransferEntryReference<'a>>
    {
        self.add_entry(dpu, MemoryTransferEntry {offset, reference: slice.into().0});
    }

    fn add_entry(&mut self, dpu: DpuId, entry: MemoryTransferEntry<'a>) {
        let (rank_id, _, _) = dpu.members();

        let rank_transfers = match self.0.entry(rank_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(MemoryTransferRankEntry::default()),
        };

        rank_transfers.0.insert(dpu, entry);
    }
}