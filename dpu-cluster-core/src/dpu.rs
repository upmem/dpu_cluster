use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DpuId {
    rank: u8,
    slice: u8,
    member: u8
}

#[derive(Debug)]
pub struct ProcessId(u64);

#[derive(Debug)]
pub struct AllocationInformation {
    owner: ProcessId
}

#[derive(Debug)]
pub struct Mapping {
    reserved: HashMap<DpuId, AllocationInformation>,
    available: Vec<DpuId>
}

impl DpuId {
    pub fn new(rank: u8, slice: u8, member: u8) -> DpuId {
        DpuId { rank, slice, member }
    }

    pub fn members(&self) -> (u8, u8, u8) {
        (self.rank, self.slice, self.member)
    }
}

impl Mapping {
    pub fn new(dpus: Vec<DpuId>) -> Self {
        Mapping {
            reserved: Default::default(),
            available: dpus
        }
    }

    pub fn reserve(&mut self, owner: ProcessId) -> Option<DpuId> {
        match self.available.pop() {
            Some(dpu) => {
                let info = AllocationInformation { owner };
                self.reserved.insert(dpu.clone(), info);
                Some(dpu)
            },
            None => None
        }
    }

    pub fn release(&mut self, dpu: &DpuId) -> Option<AllocationInformation> {
        match self.reserved.remove(dpu) {
            Some(info) => {
                self.available.push(dpu.clone());
                Some(info)
            },
            None => None
        }
    }
}