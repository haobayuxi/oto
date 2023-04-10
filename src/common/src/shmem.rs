use std::sync::atomic::{AtomicU8, Ordering};

use raw_sync::locks::{LockImpl, LockInit, RwLock as ShmemRwLock};
use shared_memory::{Shmem, ShmemConf, ShmemError};

pub fn init_shmem_lock() -> Option<Box<dyn LockImpl>> {
    // Create or open the shared memory mapping
    let shmem = match ShmemConf::new().size(1024).flink("oto").create() {
        Ok(m) => m,
        Err(ShmemError::LinkExists) => ShmemConf::new().flink("oto").open().unwrap(),
        Err(e) => return None,
    };

    let mut raw_ptr = shmem.as_ptr();
    let is_init: &mut AtomicU8;

    unsafe {
        is_init = &mut *(raw_ptr as *mut u8 as *mut AtomicU8);
        raw_ptr = raw_ptr.add(8);
    };

    // Initialize or wait for initialized mutex
    let rwlock = if shmem.is_owner() {
        is_init.store(0, Ordering::Relaxed);
        // Initialize the mutex
        let (lock, _bytes_used) = unsafe {
            ShmemRwLock::new(
                raw_ptr,                                          // Base address of Mutex
                raw_ptr.add(ShmemRwLock::size_of(Some(raw_ptr))), // Address of data protected by mutex
            )
            .unwrap()
        };
        is_init.store(1, Ordering::Relaxed);
        lock
    } else {
        // wait until mutex is initialized
        while is_init.load(Ordering::Relaxed) != 1 {}
        // Load existing mutex
        let (lock, _bytes_used) = unsafe {
            ShmemRwLock::from_existing(
                raw_ptr,                                          // Base address of Mutex
                raw_ptr.add(ShmemRwLock::size_of(Some(raw_ptr))), // Address of data  protected by mutex
            )
            .unwrap()
        };
        lock
    };

    Some(rwlock)
}
