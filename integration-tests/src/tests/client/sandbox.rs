use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_primitives::account::Account;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::{AccountId, BlockHeight, Nonce};

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

fn test_setup() -> (TestEnv, Signer) {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    assert_eq!(
        send_tx(
            &mut env,
            1,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
        ),
        ProcessTxResponse::ValidTx
    );
    do_blocks(&mut env, 1, 3);

    assert_eq!(
        send_tx(
            &mut env,
            2,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_random_value".to_string(),
                args: vec![],
                gas: 100000000000000,
                deposit: 0,
            }))],
        ),
        ProcessTxResponse::ValidTx
    );
    do_blocks(&mut env, 3, 9);
    (env, signer)
}

fn do_blocks(env: &mut TestEnv, start: BlockHeight, end: BlockHeight) {
    for i in start..end {
        let last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }
}

fn send_tx(
    env: &TestEnv,
    nonce: Nonce,
    signer_id: AccountId,
    receiver_id: AccountId,
    signer: &Signer,
    actions: Vec<Action>,
) -> ProcessTxResponse {
    let hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let tx =
        SignedTransaction::from_actions(nonce, signer_id, receiver_id, signer, actions, hash, 0);
    env.rpc_handlers[0].process_tx(tx, false, false)
}

#[test]
fn test_patch_state() {
    let (mut env, _signer) = test_setup();

    let state_item = env.query_state("test0".parse().unwrap()).swap_remove(0);
    env.clients[0].chain.patch_state(SandboxStatePatch::new(vec![StateRecord::Data {
        account_id: "test0".parse().unwrap(),
        data_key: state_item.key,
        value: b"world".to_vec().into(),
    }]));

    do_blocks(&mut env, 9, 20);
    let state = env.query_state("test0".parse().unwrap());
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].value.as_slice(), b"world");
}

#[test]
fn test_patch_account() {
    let (mut env, _signer) = test_setup();
    let mut test1: Account = env.query_account("test1".parse().unwrap()).into();
    test1.set_amount(10);

    env.clients[0].chain.patch_state(SandboxStatePatch::new(vec![StateRecord::Account {
        account_id: "test1".parse().unwrap(),
        account: test1,
    }]));
    do_blocks(&mut env, 9, 20);
    let test1_after = env.query_account("test1".parse().unwrap());
    assert_eq!(test1_after.amount, 10);
}
