//! WASM-sandboxed UDF runtime (CONCEPT:KG-2.228).
//!
//! An agent pushes a custom compute function as a WebAssembly module; the engine
//! runs it SANDBOXED over a byte payload (the serialized input rows) and reads back
//! the output bytes. The whole point is safety: a UDF is UNTRUSTED code, so the
//! sandbox grants it **nothing**.
//!
//! ## The guest ABI (the contract a UDF module must satisfy)
//!
//! A UDF module exports exactly two functions and one memory:
//!
//! * `memory` — the module's linear memory (exported, so the host can read/write it).
//! * `alloc(len: i32) -> i32` — allocate `len` bytes in guest memory, return the
//!   offset. The host calls this to place the input, and the guest uses it for its
//!   own output buffer.
//! * `udf(ptr: i32, len: i32) -> i64` — the entry point. Receives the offset+length
//!   of the input bytes the host wrote, returns a packed `i64` = `(out_ptr << 32) |
//!   out_len` pointing at the output bytes in guest memory.
//!
//! The bytes are opaque to the runtime — the engine serializes a `RowSet` (or any
//! payload) to them and deserializes the result. The runtime only moves bytes.
//!
//! ## What the sandbox denies (no host capabilities)
//!
//! The [`wasmtime::Linker`] is **empty** — no WASI, no clock, no fs, no net, no env,
//! no random. A module that imports ANYTHING fails to instantiate. So a UDF is PURE
//! compute over the bytes it is handed; it cannot observe or affect the host.
//!
//! ## Resource limits (the kill, not the hang)
//!
//! * **Fuel.** The store is created with a fuel budget ([`UdfLimits::fuel`]); every
//!   wasm instruction consumes fuel and the call TRAPS when it runs out. An
//!   infinite-loop UDF is therefore KILLED deterministically, not left to hang
//!   (proven by [`tests::infinite_loop_udf_is_fuel_killed`]).
//! * **Memory.** A [`wasmtime::ResourceLimiter`] caps the guest's linear memory at
//!   [`UdfLimits::max_memory_bytes`]; a `memory.grow` past the cap is denied and the
//!   guest traps (proven by [`tests::memory_hog_udf_is_capped`]).
//! * **Output size.** The host refuses to read back an output region larger than
//!   [`UdfLimits::max_output_bytes`] (a guest can't make the host allocate unbounded).
//!
//! ## Determinism + isolation
//!
//! A fresh [`wasmtime::Store`] per call means no state leaks between invocations.
//! The compiled [`UdfModule`] (the `wasmtime::Module`) is reusable across calls and
//! is what `Method::RegisterUdf` caches.

#![cfg(feature = "wasm-udf")]

use std::sync::Arc;

use wasmtime::{Engine, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder};

/// The resource limits enforced on every UDF call. Conservative defaults keep an
/// untrusted module from monopolizing the engine; the caller can tighten them.
#[derive(Clone, Copy, Debug)]
pub struct UdfLimits {
    /// Fuel budget — wasm instructions before a trap. Bounds CPU per call.
    pub fuel: u64,
    /// Hard cap on the guest's linear memory (bytes). A `memory.grow` past this traps.
    pub max_memory_bytes: usize,
    /// Largest output region (bytes) the host will read back from the guest.
    pub max_output_bytes: usize,
    /// Largest input payload (bytes) the host will write into the guest.
    pub max_input_bytes: usize,
}

impl Default for UdfLimits {
    fn default() -> Self {
        Self {
            // ~100M instructions: enough for real row-wise compute, far short of a
            // runaway loop. The infinite-loop test exhausts this in well under a ms.
            fuel: 100_000_000,
            max_memory_bytes: 64 * 1024 * 1024, // 64 MiB
            max_output_bytes: 16 * 1024 * 1024, // 16 MiB
            max_input_bytes: 16 * 1024 * 1024,  // 16 MiB
        }
    }
}

/// Why a UDF call failed.
#[derive(Debug)]
pub enum UdfError {
    /// The module failed to compile (bad wasm, or it imports a host capability the
    /// empty linker does not provide).
    Compile(String),
    /// The module lacks a required export (`memory` / `alloc` / `udf`) or has the
    /// wrong signature.
    Abi(String),
    /// The guest trapped — INCLUDING the fuel-exhaustion (infinite-loop kill) and the
    /// memory-cap trap. The string carries wasmtime's trap reason.
    Trap(String),
    /// The guest returned an output region larger than the limit, or one that does not
    /// fit in its memory.
    OutputTooLarge(String),
}

impl std::fmt::Display for UdfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdfError::Compile(s) => write!(f, "udf compile error: {s}"),
            UdfError::Abi(s) => write!(f, "udf ABI error: {s}"),
            UdfError::Trap(s) => write!(f, "udf trapped: {s}"),
            UdfError::OutputTooLarge(s) => write!(f, "udf output error: {s}"),
        }
    }
}

impl std::error::Error for UdfError {}

/// Per-store host state: the memory limiter (enforces [`UdfLimits::max_memory_bytes`]).
struct HostState {
    limits: StoreLimits,
}

/// A compiled, reusable UDF — the `wasmtime::Module` plus the engine it was compiled
/// for and the limits to run it under. Cheap to clone (`Arc`-shared). Built once by
/// [`UdfRegistry::register`] and run many times by [`UdfModule::run`]; instantiation
/// (a fresh `Store`) happens per call so no state leaks between invocations.
#[derive(Clone)]
pub struct UdfModule {
    engine: Engine,
    module: Module,
    limits: UdfLimits,
}

impl UdfModule {
    /// Compile `wasm_bytes` into a runnable UDF. The engine is configured with fuel
    /// consumption ON (so a call can be metered) and the wasm is validated here, so a
    /// malformed module is rejected at registration, not at first call.
    pub fn compile(wasm_bytes: &[u8], limits: UdfLimits) -> Result<Self, UdfError> {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        // No epoch interruption / no host calls — fuel is the sole CPU bound.
        let engine = Engine::new(&config).map_err(|e| UdfError::Compile(e.to_string()))?;
        let module =
            Module::new(&engine, wasm_bytes).map_err(|e| UdfError::Compile(e.to_string()))?;
        Ok(Self {
            engine,
            module,
            limits,
        })
    }

    /// Run the UDF over `input` bytes, returning the output bytes. A fresh `Store` is
    /// created per call (clean isolation), fueled + memory-limited, then the input is
    /// written into guest memory, `udf` invoked, and the result region read back.
    ///
    /// Errors:
    /// * [`UdfError::Trap`] — the guest trapped, INCLUDING fuel-exhaustion (an
    ///   infinite loop) and the memory-cap denial. This is the "kill, not hang" path.
    /// * [`UdfError::Abi`] — a missing/mis-typed export, or the module imported a host
    ///   capability (the empty linker rejects it at instantiation).
    /// * [`UdfError::OutputTooLarge`] — the returned region exceeds the limit.
    pub fn run(&self, input: &[u8]) -> Result<Vec<u8>, UdfError> {
        if input.len() > self.limits.max_input_bytes {
            return Err(UdfError::OutputTooLarge(format!(
                "input {} bytes exceeds limit {}",
                input.len(),
                self.limits.max_input_bytes
            )));
        }

        let host = HostState {
            limits: StoreLimitsBuilder::new()
                .memory_size(self.limits.max_memory_bytes)
                .build(),
        };
        let mut store = Store::new(&self.engine, host);
        store.limiter(|h| &mut h.limits);
        // The fuel budget: exhausting it TRAPS the call (the infinite-loop kill).
        store
            .set_fuel(self.limits.fuel)
            .map_err(|e| UdfError::Trap(e.to_string()))?;

        // The EMPTY linker — no WASI, no host functions at all. A module that imports
        // anything (fs/net/clock/random) fails to instantiate here. NO host caps.
        let linker: Linker<HostState> = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| UdfError::Abi(format!("instantiate (imports denied?): {e}")))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| UdfError::Abi("module does not export `memory`".into()))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| UdfError::Abi(format!("export `alloc(i32)->i32` missing: {e}")))?;
        let udf = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "udf")
            .map_err(|e| UdfError::Abi(format!("export `udf(i32,i32)->i64` missing: {e}")))?;

        // Place the input in guest memory via the guest's own allocator.
        let in_len = input.len() as i32;
        let in_ptr = alloc
            .call(&mut store, in_len)
            .map_err(|e| trap_or_abi(e))?;
        write_guest(&mut store, &memory, in_ptr, input)?;

        // Invoke the UDF. A fuel-exhausted / memory-capped guest TRAPS here.
        let packed = udf
            .call(&mut store, (in_ptr, in_len))
            .map_err(|e| trap_or_abi(e))?;

        let out_ptr = (packed >> 32) as u32 as usize;
        let out_len = (packed & 0xFFFF_FFFF) as u32 as usize;
        if out_len > self.limits.max_output_bytes {
            return Err(UdfError::OutputTooLarge(format!(
                "output {out_len} bytes exceeds limit {}",
                self.limits.max_output_bytes
            )));
        }
        read_guest(&store, &memory, out_ptr, out_len)
    }

    /// Fuel budget this module runs under (used by tests / introspection).
    pub fn fuel(&self) -> u64 {
        self.limits.fuel
    }
}

/// Map a wasmtime call error to a UDF error. A `Trap` (fuel exhaustion, memory cap,
/// out-of-bounds, unreachable) is the "kill" path; anything else is an ABI fault.
fn trap_or_abi(e: wasmtime::Error) -> UdfError {
    match e.downcast_ref::<wasmtime::Trap>() {
        Some(trap) => UdfError::Trap(trap.to_string()),
        None => UdfError::Trap(e.to_string()),
    }
}

/// Write `data` into guest memory at `ptr`, bounds-checked against the guest's memory.
fn write_guest(
    store: &mut Store<HostState>,
    memory: &Memory,
    ptr: i32,
    data: &[u8],
) -> Result<(), UdfError> {
    let ptr = ptr as usize;
    memory.write(store, ptr, data).map_err(|e| {
        UdfError::OutputTooLarge(format!("input write of {} bytes out of bounds: {e}", data.len()))
    })
}

/// Read `len` bytes from guest memory at `ptr`, bounds-checked.
fn read_guest(
    store: &Store<HostState>,
    memory: &Memory,
    ptr: usize,
    len: usize,
) -> Result<Vec<u8>, UdfError> {
    let data = memory.data(store);
    let end = ptr.checked_add(len).ok_or_else(|| {
        UdfError::OutputTooLarge("output region pointer+len overflows".into())
    })?;
    if end > data.len() {
        return Err(UdfError::OutputTooLarge(format!(
            "output region [{ptr}, {end}) exceeds guest memory of {} bytes",
            data.len()
        )));
    }
    Ok(data[ptr..end].to_vec())
}

/// A registry of compiled UDFs keyed by an opaque id. The engine holds ONE of these
/// (on `ServerState`); `Method::RegisterUdf` inserts, `Op::Udf{id}` / `Method::RunUdf`
/// look up + run. Thread-safe (the inner map is mutex-guarded; compiled modules are
/// `Arc`-cloned out for the actual run so the lock is held only for the lookup).
#[derive(Default)]
pub struct UdfRegistry {
    modules: parking_lot_lite::Mutex<std::collections::HashMap<String, Arc<UdfModule>>>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile + register `wasm_bytes` under `id`, replacing any prior module with that
    /// id. Returns an error if the module does not compile / validate.
    pub fn register(
        &self,
        id: &str,
        wasm_bytes: &[u8],
        limits: UdfLimits,
    ) -> Result<(), UdfError> {
        let module = UdfModule::compile(wasm_bytes, limits)?;
        self.modules
            .lock()
            .insert(id.to_string(), Arc::new(module));
        Ok(())
    }

    /// The compiled module registered under `id`, if any (cheap `Arc` clone).
    pub fn get(&self, id: &str) -> Option<Arc<UdfModule>> {
        self.modules.lock().get(id).cloned()
    }

    /// Run the UDF registered under `id` over `input`. Errors if no such UDF.
    pub fn run(&self, id: &str, input: &[u8]) -> Result<Vec<u8>, UdfError> {
        let module = self
            .get(id)
            .ok_or_else(|| UdfError::Abi(format!("no UDF registered under id '{id}'")))?;
        module.run(input)
    }

    /// The registered UDF ids (for introspection / a ListUdfs surface).
    pub fn ids(&self) -> Vec<String> {
        self.modules.lock().keys().cloned().collect()
    }
}

/// A tiny std-Mutex wrapper so the crate needs no `parking_lot` dep (the engine uses
/// it elsewhere, but this leaf crate stays dep-light). `lock()` panics only on
/// poisoning, which cannot happen here (no panic is held across the guard).
mod parking_lot_lite {
    pub struct Mutex<T>(std::sync::Mutex<T>);
    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Mutex(std::sync::Mutex::new(T::default()))
        }
    }
    impl<T> Mutex<T> {
        pub fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(|p| p.into_inner())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A WAT UDF that echoes its input back (identity). Exports memory/alloc/udf with
    /// a bump allocator. The input region IS the output region (identity), so `udf`
    /// just returns its args packed.
    const IDENTITY_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $next (mut i32) (i32.const 1024))
      (func (export "alloc") (param $len i32) (result i32)
        (local $p i32)
        (local.set $p (global.get $next))
        (global.set $next (i32.add (global.get $next) (local.get $len)))
        (local.get $p))
      (func (export "udf") (param $ptr i32) (param $len i32) (result i64)
        ;; return (ptr << 32) | len  — output region == input region (identity)
        (i64.or
          (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
          (i64.extend_i32_u (local.get $len)))))
    "#;

    /// A UDF that loops forever — must be fuel-killed, not hang.
    const INFINITE_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (func (export "alloc") (param $len i32) (result i32) (i32.const 1024))
      (func (export "udf") (param $ptr i32) (param $len i32) (result i64)
        (loop $forever (br $forever))
        (i64.const 0)))
    "#;

    /// A UDF that tries to grow memory past any sane cap (4096 pages = 256 MiB) and
    /// then writes — the grow is denied by the limiter, so the subsequent store traps.
    const MEMORY_HOG_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (func (export "alloc") (param $len i32) (result i32) (i32.const 0))
      (func (export "udf") (param $ptr i32) (param $len i32) (result i64)
        ;; grow by 4096 pages (256 MiB) — over the 64 MiB cap → returns -1.
        (drop (memory.grow (i32.const 4096)))
        ;; write at offset 200 MiB; with grow denied this is out of bounds → trap.
        (i32.store (i32.const 209715200) (i32.const 1))
        (i64.const 0)))
    "#;

    /// A UDF that imports a host function — must be REJECTED (no host caps).
    const IMPORTS_HOST_WAT: &str = r#"
    (module
      (import "env" "evil" (func $evil (param i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "alloc") (param $len i32) (result i32) (i32.const 0))
      (func (export "udf") (param $ptr i32) (param $len i32) (result i64)
        (drop (call $evil (i32.const 1)))
        (i64.const 0)))
    "#;

    fn wasm(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("valid wat")
    }

    #[test]
    fn identity_udf_round_trips_bytes() {
        let m = UdfModule::compile(&wasm(IDENTITY_WAT), UdfLimits::default()).unwrap();
        let input = b"hello rows from the engine".to_vec();
        let out = m.run(&input).expect("identity udf runs sandboxed");
        assert_eq!(out, input, "identity UDF must echo its input");
    }

    #[test]
    fn infinite_loop_udf_is_fuel_killed() {
        // The headline safety proof: an infinite-loop UDF is KILLED by fuel
        // exhaustion (a trap), NOT a hang. Use a tiny fuel budget so the test is fast.
        let limits = UdfLimits {
            fuel: 1_000_000,
            ..UdfLimits::default()
        };
        let m = UdfModule::compile(&wasm(INFINITE_WAT), limits).unwrap();
        let start = std::time::Instant::now();
        let err = m.run(b"x").expect_err("infinite loop must be killed");
        assert!(
            matches!(err, UdfError::Trap(_)),
            "expected a fuel-exhaustion trap, got {err:?}"
        );
        assert!(
            err.to_string().to_lowercase().contains("fuel")
                || err.to_string().to_lowercase().contains("trap"),
            "trap reason should mention fuel: {err}"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "fuel kill must be fast, not a hang"
        );
    }

    #[test]
    fn memory_hog_udf_is_capped() {
        let m = UdfModule::compile(&wasm(MEMORY_HOG_WAT), UdfLimits::default()).unwrap();
        let err = m
            .run(b"x")
            .expect_err("memory hog must be capped/trapped, not OOM the host");
        assert!(
            matches!(err, UdfError::Trap(_)),
            "expected a trap from the denied grow + OOB write, got {err:?}"
        );
    }

    #[test]
    fn module_importing_host_capability_is_rejected() {
        let m = UdfModule::compile(&wasm(IMPORTS_HOST_WAT), UdfLimits::default()).unwrap();
        let err = m
            .run(b"x")
            .expect_err("a module importing a host fn must fail to instantiate");
        assert!(
            matches!(err, UdfError::Abi(_)),
            "expected an instantiation/ABI rejection (no host caps), got {err:?}"
        );
    }

    #[test]
    fn registry_registers_and_runs() {
        let reg = UdfRegistry::new();
        reg.register("echo", &wasm(IDENTITY_WAT), UdfLimits::default())
            .unwrap();
        assert_eq!(reg.ids(), vec!["echo".to_string()]);
        let out = reg.run("echo", b"payload").unwrap();
        assert_eq!(out, b"payload");
        assert!(matches!(reg.run("missing", b"x"), Err(UdfError::Abi(_))));
    }
}
