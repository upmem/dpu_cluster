extern crate libc;

use libc::{c_void, c_uchar, c_char, c_uint, c_ushort, c_ulong};
use std::ffi::CString;
use std::collections::HashMap;
use std::vec::Vec;

// todo: we should try to generate the CNI interface. Maybe check bindgen (https://github.com/rust-lang/rust-bindgen)

#[derive(Debug, Clone)]
#[repr(C)]
enum CniStatus {
    Success,
    AllocationError,
    InvalidDpuTypeError,
    InvalidSliceIdError,
    InvalidMemberIdError,
    InvalidThreadIdError,
    InvalidNotifyIdError,
    InvalidWramAccessError,
    InvalidIramAccessError,
    InvalidMramAccessError,
    InvalidProfileError,
    CorruptedMemoryError,
    DriverError,
    SystemError,
    NotImplementedError,
    InternalError,
}

#[derive(Debug, Clone)]
pub struct DpuError(CniStatus);

#[derive(Debug)]
#[repr(C)]
pub struct DpuSignature {
    pub config_id: u32,
    pub chip_id: u32
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuStaticConfiguration {
    pub cmd_duration: u8,
    pub cmd_sampling: u8,
    pub res_duration: u8,
    pub res_sampling: u8,
    pub reset_wait_duration: u8,
    pub std_temperature: u8,
    pub clock_division: u8
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuTopology {
    pub nr_of_control_interfaces: u8,
    pub nr_of_dpus_per_control_interface: u8
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuMemoryRepair {
    pub do_iram_repair: bool,
    pub do_wram_repair: bool,
    iram_repair: *mut c_void,
    wram_repair: *mut c_void
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuMemories {
    pub mram_size: u32,
    pub wram_size: u32,
    pub iram_size: u16,
    pub dbg_mram_size: u32,
    pub repair: DpuMemoryRepair,
    pub cycle_accurate: bool
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuInfo {
    pub nr_of_threads: u8,
    pub nr_of_atomic_bits: u32,
    pub nr_of_notify_bits: u32,
    pub nr_of_work_registers_per_thread: u8,
}

#[derive(Debug)]
#[repr(C)]
pub struct DpuRankDescription {
    pub signature: DpuSignature,
    pub static_config: DpuStaticConfiguration,
    pub topology: DpuTopology,
    pub memories: DpuMemories,
    pub info: DpuInfo,
    _internals: *const c_void,
    _free_internals: *const c_void
}

#[link(name = "dpucni")]
extern {
    fn dpu_cni_get_profile_description(backend: DpuType, profile: *const c_char, description: *mut DpuRankDescription) -> CniStatus;
    fn dpu_cni_get_rank_of_type(backend: DpuType, profile: *const c_char, link: *mut*const c_void) -> CniStatus;
    fn dpu_cni_free_rank(link: *const c_void) -> CniStatus;
    fn dpu_cni_get_target_description(link: *const c_void, description: *mut DpuRankDescription) -> CniStatus;
    fn dpu_cni_reset_for_all(link: *const c_void) -> CniStatus;
    fn dpu_cni_reset_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar) -> CniStatus;
    fn dpu_cni_launch_thread_for_all(link: *const c_void, thread: c_uchar, should_resume: bool, was_running: *mut c_uint) -> CniStatus;
    fn dpu_cni_launch_thread_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, thread: c_uchar, should_resume: bool, was_running: *mut bool) -> CniStatus;
    fn dpu_cni_poll_for_all(link: *const c_void, is_running: *mut c_uint, is_in_fault: *mut c_uint) -> CniStatus;
    fn dpu_cni_poll_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, is_running: *mut bool, is_in_fault: *mut bool) -> CniStatus;
    fn dpu_cni_get_thread_status_for_all(link: *const c_void, thread: c_uchar, is_running: *mut c_uint) -> CniStatus;
    fn dpu_cni_get_thread_status_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, thread: c_uchar, is_running: *mut bool) -> CniStatus;
    fn dpu_cni_get_and_update_notify_status_for_all(link: *const c_void, notify_bit: c_uchar, value: bool, was_set: *mut c_uint) -> CniStatus;
    fn dpu_cni_get_and_update_notify_status_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, notify_bit: c_uchar, value: bool, was_set: *mut bool) -> CniStatus;
    fn dpu_cni_trigger_fault_on_all(link: *const c_void) -> CniStatus;
    fn dpu_cni_trigger_fault_on_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar) -> CniStatus;
    fn dpu_cni_copy_to_iram_for_all(link: *const c_void, to: c_ushort, source: *const c_ulong, length: c_ushort) -> CniStatus;
    fn dpu_cni_copy_to_iram_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, to: c_ushort, source: *const c_ulong, length: c_ushort) -> CniStatus;
    fn dpu_cni_copy_from_iram_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, destination: *mut c_ulong, from: c_ushort, length: c_ushort) -> CniStatus;
    fn dpu_cni_copy_to_wram_for_all(link: *const c_void, to: c_uint, source: *const c_uint, length: c_uint) -> CniStatus;
    fn dpu_cni_copy_to_wram_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, to: c_uint, source: *const c_uint, length: c_uint) -> CniStatus;
    fn dpu_cni_copy_from_wram_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, destination: *mut c_uint, from: c_uint, length: c_uint) -> CniStatus;
    fn dpu_cni_transfer_matrix_allocate(link: *const c_void, matrix: *mut*mut c_void) -> CniStatus;
    fn dpu_cni_transfer_matrix_free(link: *const c_void, matrix: *mut c_void) -> ();
    fn dpu_cni_transfer_matrix_add_dpu(link: *const c_void, matrix: *mut c_void, slice_id: c_uchar, member_id: c_uchar, buffer: *mut c_uchar, length: c_uint, offset: c_uint, mram_number: c_uint) -> ();
    fn dpu_cni_transfer_matrix_clear_dpu(link: *const c_void, matrix: *mut c_void, slice_id: c_uchar, member_id: c_uchar) -> ();
    fn dpu_cni_transfer_matrix_clear_all(link: *const c_void, matrix: *mut c_void) -> ();
    fn dpu_cni_copy_to_mram_number_for_dpus(link: *const c_void, matrix: *const c_void) -> CniStatus;
    fn dpu_cni_copy_from_mram_number_for_dpus(link: *const c_void, matrix: *const c_void) -> CniStatus;
    fn dpu_cni_copy_to_mram_number_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, to: c_uint, source: *const c_uchar, length: c_uint, mram_number: c_uint) -> CniStatus;
    fn dpu_cni_copy_from_mram_number_for_dpu(link: *const c_void, slice_id: c_uchar, member_id: c_uchar, destination: *mut c_uchar, from: c_uint, length: c_uint, mram_number: c_uint) -> CniStatus;
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub enum DpuType {
    FunctionalSimulator,
    Hardware
}

impl Default for DpuType {
    fn default() -> Self {
        DpuType::Hardware
    }
}

#[derive(Default, Debug)]
pub struct DpuProfile {
    properties: HashMap<String, String>
}

#[derive(Default, Debug)]
pub struct DpuTarget {
    dpu_type: DpuType,
    profile: DpuProfile
}

impl DpuTarget {
    pub fn for_functional_simulator() -> Self {
        DpuTarget {
            dpu_type: DpuType::FunctionalSimulator,
            profile: DpuProfile {
                properties: HashMap::default()
            }
        }
    }

    pub fn for_hardware_implementation() -> Self {
        DpuTarget {
            dpu_type: DpuType::Hardware,
            profile: DpuProfile {
                properties: HashMap::default()
            }
        }
    }

    pub fn to_cni_args(&self) -> (DpuType, String) {
        let profile_str = self.profile.properties.iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect::<Vec<String>>()
            .join("&");
        (self.dpu_type, profile_str)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct DpuRank(*const c_void);

#[derive(Debug)]
pub struct DpuRankTransferMatrix<'a> {
    matrix: *mut c_void,
    rank: &'a DpuRank
}

fn wrap_cni_result<R>(result: R, status: CniStatus) -> Result<R, DpuError> {
    match status {
        CniStatus::Success => Ok(result),
        _ => Err(DpuError(status))
    }
}

impl DpuRank {
    pub fn find_nr_of_available_dpus_for(dpu_type: DpuType, profile: &str) -> Result<u32, DpuError> {
        // todo

        let nr_of_dpus = match dpu_type {
            DpuType::FunctionalSimulator => 8,
            DpuType::Hardware => 0,
        };

        Ok(nr_of_dpus)
    }
    
    pub fn get_description_for(dpu_type: DpuType, profile: &str) -> Result<DpuRankDescription, DpuError> {
        // unwrap: CString::new cannot return an error with a Rust String as argument
        let c_profile = CString::new(profile).unwrap();

        let mut description;

        let status = unsafe {
            description = std::mem::uninitialized();
            dpu_cni_get_profile_description(dpu_type, c_profile.as_ptr(), &mut description)
        };

        wrap_cni_result(description, status)
    }

    pub fn allocate_for(dpu_type: DpuType, profile: &str) -> Result<DpuRank, DpuError> {
        let mut link = std::ptr::null();
        // unwrap: CString::new cannot return an error with a Rust String as argument
        let c_profile = CString::new(profile).unwrap();

        let status = unsafe { dpu_cni_get_rank_of_type(dpu_type, c_profile.as_ptr(), &mut link) };

        wrap_cni_result(DpuRank(link), status)
    }

    pub fn get_description(&self) -> Result<DpuRankDescription, DpuError> {
        let mut description;

        let status = unsafe {
            description = std::mem::uninitialized();
            dpu_cni_get_target_description(self.0, &mut description)
        };

        wrap_cni_result(description, status)
    }

    pub fn reset_all(&self) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_reset_for_all(self.0) };

        wrap_cni_result((), status)
    }

    pub fn reset_dpu(&self, slice_id: u8, member_id: u8) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_reset_for_dpu(self.0, slice_id, member_id) };

        wrap_cni_result((), status)
    }

    pub fn launch_thread_on_all(&self, thread: u8, should_resume: bool, was_running: *mut u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_launch_thread_for_all(self.0, thread, should_resume, was_running) };

        wrap_cni_result((), status)
    }

    pub fn launch_thread_on_dpu(&self, slice_id: u8, member_id: u8, thread: u8, should_resume: bool, was_running: *mut bool) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_launch_thread_for_dpu(self.0, slice_id, member_id, thread, should_resume, was_running) };

        wrap_cni_result((), status)
    }

    pub fn poll_all(&self, is_running: *mut u32, is_in_fault: *mut u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_poll_for_all(self.0, is_running, is_in_fault) };

        wrap_cni_result((), status)
    }

    pub fn poll_dpu(&self, slice_id: u8, member_id: u8, is_running: *mut bool, is_in_fault: *mut bool) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_poll_for_dpu(self.0, slice_id, member_id, is_running, is_in_fault) };

        wrap_cni_result((), status)
    }

    pub fn fetch_thread_status_on_all(&self, thread: u8, is_running: *mut u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_get_thread_status_for_all(self.0, thread, is_running) };

        wrap_cni_result((), status)
    }

    pub fn fetch_thread_status_on_dpu(&self, slice_id: u8, member_id: u8, thread: u8, is_running: *mut bool) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_get_thread_status_for_dpu(self.0, slice_id, member_id, thread, is_running) };

        wrap_cni_result((), status)
    }

    pub fn get_and_update_notification_on_all(&self, notify: u8, update: bool, was_set: *mut u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_get_and_update_notify_status_for_all(self.0, notify, update, was_set) };

        wrap_cni_result((), status)
    }

    pub fn get_and_update_notification_on_dpu(&self, slice_id: u8, member_id: u8, notify: u8, update: bool, was_set: *mut bool) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_get_and_update_notify_status_for_dpu(self.0, slice_id, member_id, notify, update, was_set) };

        wrap_cni_result((), status)
    }

    pub fn fault_all(&self) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_trigger_fault_on_all(self.0) };

        wrap_cni_result((), status)
    }

    pub fn fault_dpu(&self, slice_id: u8, member_id: u8) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_trigger_fault_on_dpu(self.0, slice_id, member_id) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_irams(&self, buffer: *const u64, length: u16, offset: u16) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_iram_for_all(self.0, offset, buffer, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_iram(&self, slice_id: u8, member_id: u8, buffer: *const u64, length: u16, offset: u16) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_iram_for_dpu(self.0, slice_id, member_id, offset, buffer, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_from_iram(&self, slice_id: u8, member_id: u8, buffer: *mut u64, length: u16, offset: u16) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_from_iram_for_dpu(self.0, slice_id, member_id, buffer, offset, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_wrams(&self, buffer: *const u32, length: u32, offset: u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_wram_for_all(self.0, offset, buffer, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_wram(&self, slice_id: u8, member_id: u8, buffer: *const u32, length: u32, offset: u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_wram_for_dpu(self.0, slice_id, member_id, offset, buffer, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_from_wram(&self, slice_id: u8, member_id: u8, buffer: *mut u32, length: u32, offset: u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_from_wram_for_dpu(self.0, slice_id, member_id, buffer, offset, length) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_mrams(&self, matrix: &DpuRankTransferMatrix) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_mram_number_for_dpus(self.0, matrix.matrix) };

        wrap_cni_result((), status)
    }

    pub fn copy_to_mram(&self, slice_id: u8, member_id: u8, buffer: *const u8, length: u32, offset: u32, mram_number: u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_to_mram_number_for_dpu(self.0, slice_id, member_id, offset, buffer, length, mram_number) };

        wrap_cni_result((), status)
    }

    pub fn copy_from_mrams(&self, matrix: &DpuRankTransferMatrix) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_from_mram_number_for_dpus(self.0, matrix.matrix) };

        wrap_cni_result((), status)
    }

    pub fn copy_from_mram(&self, slice_id: u8, member_id: u8, buffer: *mut u8, length: u32, offset: u32, mram_number: u32) -> Result<(), DpuError> {
        let status = unsafe { dpu_cni_copy_from_mram_number_for_dpu(self.0, slice_id, member_id, buffer, offset, length, mram_number) };

        wrap_cni_result((), status)
    }

    pub fn free(self) -> () {
        // drop will do the job
    }
}

impl Drop for DpuRank {
    fn drop(&mut self) {
        unsafe { dpu_cni_free_rank(self.0); }
    }
}

unsafe impl Send for DpuRank {}
unsafe impl Sync for DpuRank {}

impl <'a> DpuRankTransferMatrix<'a> {
    pub fn allocate_for(rank: &'a DpuRank) -> Result<DpuRankTransferMatrix<'a>, DpuError> {
        let DpuRank(link) = *rank;
        let mut matrix = std::ptr::null_mut();

        let status = unsafe { dpu_cni_transfer_matrix_allocate(link, &mut matrix) };

        wrap_cni_result(DpuRankTransferMatrix { matrix, rank }, status)
    }

    pub fn add_dpu(&self, slice_id: u8, member_id: u8, buffer: *mut u8, length: u32, offset: u32, mram_number: u32) -> () {
        unsafe { dpu_cni_transfer_matrix_add_dpu(self.rank.0, self.matrix, slice_id, member_id, buffer, length, offset, mram_number) }
    }

    pub fn clear_dpu(&self, slice_id: u8, member_id: u8) -> () {
        unsafe { dpu_cni_transfer_matrix_clear_dpu(self.rank.0, self.matrix, slice_id, member_id) }
    }

    pub fn clear_all(&self) -> () {
        unsafe { dpu_cni_transfer_matrix_clear_all(self.rank.0, self.matrix) }
    }

    pub fn free(self) -> () {
        // drop will do the job
    }
}

impl <'a> Drop for DpuRankTransferMatrix<'a> {
    fn drop(&mut self) {
        unsafe { dpu_cni_transfer_matrix_free(self.rank.0, self.matrix); }
    }
}

