extern crate dpu_cluster_core;

use dpu_cluster_core::cluster::Cluster;
use dpu_cluster_core::config::ClusterConfiguration;
use dpu_cluster_core::error::ClusterError;
use dpu_cluster_core::driver::Driver;
use dpu_cluster_core::view::View;
use dpu_cluster_core::program::Program;
use std::collections::HashMap;
use std::io;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::BufReader;
use std::io::BufRead;
use dpu_cluster_core::dpu::DpuId;
use dpu_cluster_core::memory::MemoryTransfer;

const NB_WORD_MAX: u32 = 7450;

const NB_WORD_SIZE: u32 = 8;
const STATS_SIZE: u32 = 64;
const ADDRESSES_SIZE: u32 = NB_WORD_MAX * 4;

const NB_OF_WORDS_OFFSET: u32 = 0;
const STATS_OFFSET: u32 = NB_OF_WORDS_OFFSET + NB_WORD_SIZE;
const ADDRESSES_OFFSET: u32 = STATS_OFFSET + STATS_SIZE;
const STRINGS_OFFSET: u32 = ADDRESSES_OFFSET + ADDRESSES_SIZE;

// The DPU program and the input files can be generated in the main sort_strings repo
const DPU_PROGRAM_IRAM: &'static str = "dpu.iram";
const DPU_PROGRAM_WRAM: &'static str = "dpu.wram";

const INPUT_FILE: &'static str = "input.txt";
const OUTPUT_FILE: &'static str = "output.txt";

#[derive(Debug)]
enum AppError {
    DpuError(ClusterError),
    FileManagementError(io::Error),
    InvalidStringEntry(u32),
    InputFileTooBig(usize)
}

fn main() -> Result<(), AppError> {
    let config = ClusterConfiguration::for_functional_simulator(1);
    let cluster = Cluster::create(config)?;
    let driver = cluster.driver();

    do_sort(driver, INPUT_FILE, OUTPUT_FILE)
}

fn do_sort(driver: &Driver, input_file: &str, output_file: &str) -> Result<(), AppError> {
    let mram_size = driver.rank_description.memories.mram_size;
    let dpu = DpuId::new(0, 0, 0);
    let view = View::one(dpu);
    let program = fetch_dpu_program()?;
    let (mut strings, mut addresses, string_map) = extract_inputs(input_file, mram_size)?;
    let mut nb_of_words = vec![addresses.len() as u32, 0];

    driver.load(&view, &program)?;

    {
        let mut input_tranfers = prepare_input_memory_transfers(dpu, &mut strings, &mut addresses, &mut nb_of_words);
        for transfer in input_tranfers.iter_mut() {
            driver.copy_to_memory(transfer)?;
        }
    }

    driver.run(&view)?;

    {
        let mut output_tranfer = prepare_output_memory_transfer(dpu, &mut addresses);
        driver.copy_from_memory(&mut output_tranfer)?;
    }

    process_outputs(addresses, output_file, string_map)
}

fn fetch_dpu_program() -> Result<Program, AppError> {
    Ok(Program::new_raw(std::fs::read(DPU_PROGRAM_IRAM)?, std::fs::read(DPU_PROGRAM_WRAM)?))
}

fn extract_inputs(filename: &str, mram_size: u32) -> Result<(Vec<u8>, Vec<u32>, HashMap<u32, String>), AppError> {
    let file = File::open(filename)?;
    let file = BufReader::new(file);

    let mut string_map = HashMap::default();
    let mut string_addresses = Vec::default();
    let mut strings = Vec::default();

    for line in file.lines() {
        let offset = strings.len();
        let line = line?;

        strings.extend(line.as_bytes());
        strings.push(b'\0');

        let end = strings.len();
        strings.resize((end + 7) & !7, b'\0');

        string_addresses.push(offset as u32);
        string_map.insert(offset as u32, line);
    }

    if strings.len() > ((mram_size - STRINGS_OFFSET) as usize) {
        return Err(AppError::InputFileTooBig(strings.len()))
    }

    Ok((strings, string_addresses, string_map))
}

fn prepare_input_memory_transfers<'a>(dpu: DpuId, strings: &'a mut [u8], addresses: &'a mut [u32], nb_of_words: &'a mut [u32]) -> Vec<MemoryTransfer<'a>> {
    let strings_tranfer = MemoryTransfer::default().add(dpu, STRINGS_OFFSET, strings);
    let addresses_tranfer = MemoryTransfer::default().add(dpu, ADDRESSES_OFFSET, addresses);
    let nb_of_words_tranfer = MemoryTransfer::default().add(dpu, NB_OF_WORDS_OFFSET, nb_of_words);

    vec![strings_tranfer, addresses_tranfer, nb_of_words_tranfer]
}

fn prepare_output_memory_transfer(dpu: DpuId, output: &mut [u32]) -> MemoryTransfer {
    MemoryTransfer::default().add(dpu, ADDRESSES_OFFSET, output)
}

fn process_outputs(output: Vec<u32>, filename: &str, string_map: HashMap<u32, String>) -> Result<(), AppError> {
    let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(filename)?;

    for entry in output {
        println!("{}", entry);
        let string = string_map.get(&entry).ok_or_else(|| AppError::InvalidStringEntry(entry))?;
        file.write(string.as_bytes())?;
        file.write(b"\n")?;
    }

    Ok(())
}

impl From<ClusterError> for AppError {
    fn from(err: ClusterError) -> Self {
        AppError::DpuError(err)
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> Self {
        AppError::FileManagementError(err)
    }
}