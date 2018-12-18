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

    pub fn new_raw(iram: Vec<u8>, wram: Vec<u8>) -> Program {
        let iram = iram.chunks(8).map(|chunk| chunk.iter().fold((0 as u64, 0), |(acc, i), b| (acc | ((*b as u64) << i), i + 8))).map(|(x, _)| x).collect();
        let wram = wram.chunks(4).map(|chunk| chunk.iter().fold((0 as u32, 0), |(acc, i), b| (acc | ((*b as u32) << i), i + 8))).map(|(x, _)| x).collect();

        Program::new(iram, wram, None)
    }
}