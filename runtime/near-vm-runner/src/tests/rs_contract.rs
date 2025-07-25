use crate::ContractCode;
use crate::logic::Config;
use crate::logic::errors::{FunctionCallError, HostError, WasmTrap};
use crate::logic::mocks::mock_external::{MockAction, MockedExternal};
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::types::Balance;
use std::mem::size_of;
use std::sync::Arc;

use super::test_vm_config;
use crate::runner::VMResult;
use crate::tests::{
    CURRENT_ACCOUNT_ID, PREDECESSOR_ACCOUNT_ID, SIGNER_ACCOUNT_ID, SIGNER_ACCOUNT_PK,
    create_context, with_vm_variants,
};
use near_parameters::vm::VMKind;

/// Encode array of `u64` to be passed as a smart contract argument.
fn encode(xs: &[u64]) -> Vec<u8> {
    xs.iter().flat_map(|it| it.to_le_bytes()).collect()
}

fn test_contract(vm_kind: VMKind) -> ContractCode {
    let code = match vm_kind {
        VMKind::Wasmer0 => unreachable!(),
        VMKind::Wasmer2 => unreachable!(),
        // production and developer environment, use a cutting-edge WASM
        VMKind::Wasmtime | VMKind::NearVm | VMKind::NearVm2 => near_test_contracts::rs_contract(),
    };
    ContractCode::new(code.to_vec(), None)
}

#[track_caller]
fn assert_run_result(result: VMResult, expected_value: u64) {
    let outcome = result.expect("Failed execution");

    if let ReturnData::Value(value) = &outcome.return_data {
        let mut arr = [0u8; size_of::<u64>()];
        arr.copy_from_slice(&value);
        let res = u64::from_le_bytes(arr);
        assert_eq!(res, expected_value);
    } else {
        panic!("Value was not returned");
    }
}

#[test]
pub fn test_read_write() {
    with_vm_variants(|vm_kind: VMKind| {
        let config = Arc::new(test_vm_config(Some(vm_kind)));
        let fees = Arc::new(RuntimeFeesConfig::test());
        let code = test_contract(vm_kind);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context(encode(&[10u64, 20u64]));

        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        let result = runtime.prepare(&fake_external, None, gas_counter, "write_key_value").run(
            &mut fake_external,
            &context,
            Arc::clone(&fees),
        );
        assert_run_result(result, 0);

        let context = create_context(encode(&[10u64]));
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        let result = runtime.prepare(&fake_external, None, gas_counter, "read_value").run(
            &mut fake_external,
            &context,
            Arc::clone(&fees),
        );
        assert_run_result(result, 20);
    });
}

macro_rules! def_test_ext {
    ($name:ident, $method:expr, $expected:expr, $input:expr, $validator:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                let config = Arc::new(test_vm_config(Some(vm_kind)));
                run_test_ext(Arc::clone(&config), $method, $expected, $input, $validator, vm_kind)
            });
        }
    };
    ($name:ident, $method:expr, $expected:expr, $input:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                let config = Arc::new(test_vm_config(Some(vm_kind)));
                run_test_ext(Arc::clone(&config), $method, $expected, $input, vec![], vm_kind)
            });
        }
    };
    ($name:ident, $method:expr, $expected:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                let config = Arc::new(test_vm_config(Some(vm_kind)));
                run_test_ext(Arc::clone(&config), $method, $expected, &[], vec![], vm_kind)
            })
        }
    };
}

fn run_test_ext(
    config: Arc<Config>,
    method: &str,
    expected: &[u8],
    input: &[u8],
    validators: Vec<(&str, Balance)>,
    vm_kind: VMKind,
) {
    let code = test_contract(vm_kind);
    let mut fake_external = MockedExternal::with_code(code);
    fake_external.validators =
        validators.into_iter().map(|(s, b)| (s.parse().unwrap(), b)).collect();
    let fees = Arc::new(RuntimeFeesConfig::test());
    let context = create_context(input.to_vec());
    let gas_counter = context.make_gas_counter(&config);
    let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");
    let outcome = runtime
        .prepare(&fake_external, None, gas_counter, &method)
        .run(&mut fake_external, &context, Arc::clone(&fees))
        .unwrap_or_else(|err| panic!("Failed execution: {:?}", err));

    assert_eq!(outcome.profile.action_gas(), 0);

    if let ReturnData::Value(value) = outcome.return_data {
        assert_eq!(&value, &expected);
    } else {
        panic!("Value was not returned, got outcome {:?}", outcome);
    }
}

def_test_ext!(ext_account_id, "ext_account_id", CURRENT_ACCOUNT_ID.as_bytes());

def_test_ext!(ext_signer_id, "ext_signer_id", SIGNER_ACCOUNT_ID.as_bytes());
def_test_ext!(
    ext_predecessor_account_id,
    "ext_predecessor_account_id",
    PREDECESSOR_ACCOUNT_ID.as_bytes(),
    &[]
);
def_test_ext!(ext_signer_pk, "ext_signer_pk", &SIGNER_ACCOUNT_PK);
def_test_ext!(ext_random_seed, "ext_random_seed", &[0, 1, 2]);

def_test_ext!(ext_prepaid_gas, "ext_prepaid_gas", &(10_u64.pow(14)).to_le_bytes());
def_test_ext!(ext_block_index, "ext_block_index", &10u64.to_le_bytes());
def_test_ext!(ext_block_timestamp, "ext_block_timestamp", &42u64.to_le_bytes());
def_test_ext!(ext_storage_usage, "ext_storage_usage", &12u64.to_le_bytes());

#[test]
pub fn ext_used_gas() {
    with_vm_variants(|vm_kind: VMKind| {
        let config = Arc::new(test_vm_config(Some(vm_kind)));
        // Note, the used_gas is not a global used_gas at the beginning of method, but instead a
        // diff in used_gas for computing fib(30) in a loop
        let expected = [27, 180, 237, 15, 0, 0, 0, 0];
        run_test_ext(config, "ext_used_gas", &expected, &[], vec![], vm_kind)
    })
}

def_test_ext!(
    ext_sha256,
    "ext_sha256",
    &[
        18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112, 92,
        255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
    ],
    b"tesdsst"
);
// current_account_balance = context.account_balance + context.attached_deposit;
def_test_ext!(ext_account_balance, "ext_account_balance", &(2u128 + 2).to_le_bytes());
def_test_ext!(ext_attached_deposit, "ext_attached_deposit", &2u128.to_le_bytes());

def_test_ext!(
    ext_validator_stake_alice,
    "ext_validator_stake",
    &(100u128).to_le_bytes(),
    b"alice",
    vec![("alice", 100), ("bob", 1)]
);
def_test_ext!(
    ext_validator_stake_bob,
    "ext_validator_stake",
    &(1u128).to_le_bytes(),
    b"bob",
    vec![("alice", 100), ("bob", 1)]
);
def_test_ext!(
    ext_validator_stake_carol,
    "ext_validator_stake",
    &(0u128).to_le_bytes(),
    b"carol",
    vec![("alice", 100), ("bob", 1)]
);

def_test_ext!(
    ext_validator_total_stake,
    "ext_validator_total_stake",
    &(100u128 + 1).to_le_bytes(),
    &[],
    vec![("alice", 100), ("bob", 1)]
);

#[test]
pub fn test_out_of_memory() {
    with_vm_variants(|vm_kind: VMKind| {
        // TODO: currently we only run this test on near-vm and wasmtime
        match vm_kind {
            VMKind::Wasmer2 | VMKind::Wasmer0 => return,
            _ => {}
        }

        let mut config = test_vm_config(Some(vm_kind));
        config.make_free();
        let config = Arc::new(config);
        let code = test_contract(vm_kind);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context(Vec::new());
        let fees = Arc::new(RuntimeFeesConfig::free());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        let result = runtime
            .prepare(&fake_external, None, gas_counter, "out_of_memory")
            .run(&mut fake_external, &context, fees)
            .expect("execution failed");
        assert_eq!(
            result.aborted,
            match vm_kind {
                VMKind::NearVm | VMKind::NearVm2 | VMKind::Wasmtime =>
                    Some(FunctionCallError::WasmTrap(WasmTrap::Unreachable)),
                VMKind::Wasmer2 | VMKind::Wasmer0 => unreachable!(),
            }
        );
    })
}

fn function_call_weight_contract() -> ContractCode {
    ContractCode::new(near_test_contracts::rs_contract().to_vec(), None)
}

#[test]
fn attach_unspent_gas_but_use_all_gas() {
    with_vm_variants(|vm_kind: VMKind| {
        let mut context = create_context(vec![]);
        context.prepaid_gas = 100 * 10u64.pow(12);

        let mut config = test_vm_config(Some(vm_kind));
        config.limit_config.max_gas_burnt = context.prepaid_gas / 3;
        let config = Arc::new(config);
        let code = function_call_weight_contract();
        let mut external = MockedExternal::with_code(code);
        let fees = Arc::new(RuntimeFeesConfig::test());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");

        let gas_counter = context.make_gas_counter(&config);
        let outcome = runtime
            .prepare(&external, None, gas_counter, "attach_unspent_gas_but_use_all_gas")
            .run(&mut external, &context, fees)
            .unwrap_or_else(|err| panic!("Failed execution: {:?}", err));

        let err = outcome.aborted.as_ref().unwrap();
        assert!(matches!(err, FunctionCallError::HostError(HostError::GasExceeded)));

        match &external.action_log[..] {
            [_, MockAction::FunctionCallWeight { prepaid_gas: gas, .. }, _] => assert_eq!(*gas, 0),
            other => panic!("unexpected actions: {other:?}"),
        }
    });
}
