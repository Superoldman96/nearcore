use near_parameters::vm;
#[allow(unused_imports)]
use opts::*;

#[allow(dead_code)]
mod opts {
    pub(super) const MULTI_VALUE: bool = false;
    pub(super) const SIMD: bool = false;
    pub(super) const THREADS: bool = false;
    pub(super) const TAIL_CALL: bool = false;
    pub(super) const MULTI_MEMORY: bool = false;
    pub(super) const MEMORY64: bool = false;
    pub(super) const SATURATING_FLOAT_TO_INT: bool = false;
    pub(super) const EXCEPTIONS: bool = false;
    pub(super) const RELAXED_SIMD: bool = false;
    pub(super) const EXTENDED_COST: bool = false;
    pub(super) const COMPONENT_MODEL: bool = false;
    pub(super) const GC: bool = false;
    pub(super) const FUNCTION_REFERENCES: bool = false;
    pub(super) const MEMORY_CONTROL: bool = false;
    pub(super) const SIGN_EXTENSION: bool = true;
    pub(super) const STACK_SWITCHING: bool = false;
    pub(super) const WIDE_ARITHMETIC: bool = false;
    pub(super) const CUSTOM_PAGE_SIZES: bool = false;
}

#[derive(Clone, Copy)]
#[allow(unused)]
pub struct WasmFeatures {
    saturating_float_to_int: bool,
    reftypes_bulk_memory: bool,
}

impl WasmFeatures {
    #[allow(unused)]
    pub fn new(config: &vm::Config) -> Self {
        Self {
            saturating_float_to_int: config.saturating_float_to_int,
            reftypes_bulk_memory: config.reftypes_bulk_memory,
        }
    }
}

#[cfg(feature = "finite-wasm")]
impl From<WasmFeatures> for finite_wasm::wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        assert!(!f.reftypes_bulk_memory);
        finite_wasm::wasmparser::WasmFeatures {
            floats: true,
            mutable_global: true,
            sign_extension: SIGN_EXTENSION,
            saturating_float_to_int: f.saturating_float_to_int,
            reference_types: f.reftypes_bulk_memory,
            bulk_memory: f.reftypes_bulk_memory,

            // wasmer singlepass compiler requires multi_value return values to be disabled.
            multi_value: MULTI_VALUE,
            simd: SIMD,
            threads: THREADS,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            exceptions: EXCEPTIONS,
            memory64: MEMORY64,
            relaxed_simd: RELAXED_SIMD,
            extended_const: EXTENDED_COST,
            component_model: COMPONENT_MODEL,
            function_references: FUNCTION_REFERENCES,
            memory_control: MEMORY_CONTROL,
            gc: GC,
        }
    }
}

#[cfg(feature = "finite-wasm-6")]
impl From<WasmFeatures> for finite_wasm_6::wasmparser::WasmFeatures {
    fn from(f: WasmFeatures) -> Self {
        finite_wasm_6::wasmparser::WasmFeaturesInflated {
            floats: true,
            mutable_global: true,
            sign_extension: SIGN_EXTENSION,
            saturating_float_to_int: f.saturating_float_to_int,
            reference_types: f.reftypes_bulk_memory,
            bulk_memory: f.reftypes_bulk_memory,

            // wasmer singlepass compiler requires multi_value return values to be disabled.
            multi_value: MULTI_VALUE,
            simd: SIMD,
            threads: THREADS,
            shared_everything_threads: THREADS,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            exceptions: EXCEPTIONS,
            legacy_exceptions: EXCEPTIONS,
            memory64: MEMORY64,
            relaxed_simd: RELAXED_SIMD,
            extended_const: EXTENDED_COST,
            component_model: COMPONENT_MODEL,
            cm_values: COMPONENT_MODEL,
            cm_nested_names: COMPONENT_MODEL,
            cm_async: COMPONENT_MODEL,
            cm_async_stackful: COMPONENT_MODEL,
            cm_async_builtins: COMPONENT_MODEL,
            function_references: FUNCTION_REFERENCES,
            memory_control: MEMORY_CONTROL,
            gc: GC,
            gc_types: GC,
            custom_page_sizes: CUSTOM_PAGE_SIZES,
            stack_switching: STACK_SWITCHING,
            wide_arithmetic: WIDE_ARITHMETIC,
        }
        .into()
    }
}

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
impl From<WasmFeatures> for near_vm_types::Features {
    fn from(f: crate::features::WasmFeatures) -> Self {
        Self {
            mutable_global: true,
            saturating_float_to_int: f.saturating_float_to_int,
            reference_types: f.reftypes_bulk_memory,
            bulk_memory: f.reftypes_bulk_memory,

            sign_extension: SIGN_EXTENSION,
            threads: THREADS,
            simd: SIMD,
            multi_value: MULTI_VALUE,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            memory64: MEMORY64,
            exceptions: EXCEPTIONS,
        }
    }
}

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
impl From<WasmFeatures> for near_vm_2_types::Features {
    fn from(f: crate::features::WasmFeatures) -> Self {
        Self {
            mutable_global: true,
            saturating_float_to_int: f.saturating_float_to_int,
            reference_types: f.reftypes_bulk_memory,
            bulk_memory: f.reftypes_bulk_memory,

            sign_extension: SIGN_EXTENSION,
            threads: THREADS,
            simd: SIMD,
            multi_value: MULTI_VALUE,
            tail_call: TAIL_CALL,
            multi_memory: MULTI_MEMORY,
            memory64: MEMORY64,
            exceptions: EXCEPTIONS,
        }
    }
}

#[cfg(feature = "wasmtime_vm")]
impl From<WasmFeatures> for wasmtime::Config {
    fn from(_: WasmFeatures) -> Self {
        // preparation code did all the filtering necessary already. Default configuration supports
        // all the necessary features (and, yes, enables more of them.)
        wasmtime::Config::default()
    }
}
