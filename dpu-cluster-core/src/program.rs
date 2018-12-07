use std::collections::HashMap;

type IramAddress = u16;
type WramAddress = u32;

type Instruction = u64;
type WramData = u32;

pub struct Program {
    pub iram_sections: HashMap<IramAddress, Vec<Instruction>>,
    pub wram_sections: HashMap<WramAddress, Vec<WramData>>,
    binary_file: Option<String>
}

impl Program {
    pub fn new(iram: Vec<Instruction>, wram: Vec<WramData>, binary_file: Option<String>) -> Program {
        let mut iram_sections = HashMap::default();
        let mut wram_sections = HashMap::default();
        iram_sections.insert(0, iram);
        wram_sections.insert(0, wram);

        Program { iram_sections, wram_sections, binary_file }
    }
}