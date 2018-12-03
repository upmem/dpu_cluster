use std::collections::HashMap;
use LoaderError;

type IramAddress = u16;
type WramAddress = u32;

type Instruction = u64;
type WramData = u32;

pub struct Program {
    pub iram_sections: HashMap<IramAddress, Vec<Instruction>>,
    pub wram_sections: HashMap<WramAddress, Vec<WramData>>,
    origin_file: String
}

impl Program {
    pub fn from_file(file: &str) -> Result<Program, LoaderError> {
        unimplemented!()
    }
}