use assert_matches::assert_matches;
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::errors::TxExecutionError;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, Transaction, TransactionV0};
use near_primitives::types::BlockHeight;
use near_primitives::views::FinalExecutionStatus;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use crate::utils::process_blocks::{
    deploy_test_contract_with_protocol_version, produce_blocks_from_height_with_protocol_version,
};

/// Check correctness of the protocol upgrade and ability to write 2 KB keys.
#[test]
fn protocol_upgrade() {
    init_test_logger();

    let old_protocol_version =
        near_primitives::version::ProtocolFeature::LowerStorageKeyLimit.protocol_version() - 1;
    let new_storage_key_limit = 2usize.pow(11); // 2 KB
    let args: Vec<u8> = vec![1u8; new_storage_key_limit + 1]
        .into_iter()
        .chain(near_primitives::test_utils::encode(&[10u64]).into_iter())
        .collect();
    let epoch_length: BlockHeight = 5;

    // The immediate protocol upgrade needs to be set for this test to pass in
    // the release branch where the protocol upgrade date is set.
    unsafe { std::env::set_var("NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE", "now") };

    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let mut env = TestEnv::builder(&genesis.config)
            .track_all_shards()
            .nightshade_runtimes_with_runtime_config_store(
                &genesis,
                vec![RuntimeConfigStore::new(None)],
            )
            .build();

        deploy_test_contract_with_protocol_version(
            &mut env,
            "test0".parse().unwrap(),
            near_test_contracts::backwards_compatible_rs_contract(),
            epoch_length,
            1,
            old_protocol_version,
        );
        env
    };

    let signer: Signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = TransactionV0 {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "write_key_value".to_string(),
            args,
            gas: 10u64.pow(14),
            deposit: 0,
        }))],

        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Run transaction writing storage key exceeding the limit. Check that execution succeeds.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx = Transaction::V0(TransactionV0 {
            nonce: tip.height + 1,
            block_hash: tip.last_block_hash,
            ..tx.clone()
        })
        .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        assert_eq!(
            env.tx_request_handlers[0].process_tx(signed_tx, false, false),
            ProcessTxResponse::ValidTx
        );
        produce_blocks_from_height_with_protocol_version(
            &mut env,
            epoch_length,
            tip.height + 1,
            old_protocol_version,
        );
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    }

    env.upgrade_protocol_to_latest_version();

    // Re-run the transaction, check that execution fails.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx = Transaction::V0(TransactionV0 {
            nonce: tip.height + 1,
            block_hash: tip.last_block_hash,
            ..tx
        })
        .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        assert_eq!(
            env.tx_request_handlers[0].process_tx(signed_tx, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..epoch_length {
            let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
        }
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(
            final_result.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(_))
        );
    }

    // Run transaction where storage key exactly fits the new limit, check that execution succeeds.
    {
        let args: Vec<u8> = vec![1u8; new_storage_key_limit]
            .into_iter()
            .chain(near_primitives::test_utils::encode(&[20u64]).into_iter())
            .collect();
        let tx = TransactionV0 {
            signer_id: "test0".parse().unwrap(),
            receiver_id: "test0".parse().unwrap(),
            public_key: signer.public_key(),
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_key_value".to_string(),
                args,
                gas: 10u64.pow(14),
                deposit: 0,
            }))],

            nonce: 0,
            block_hash: CryptoHash::default(),
        };
        let tip = env.clients[0].chain.head().unwrap();
        let signed_tx = Transaction::V0(TransactionV0 {
            nonce: tip.height + 1,
            block_hash: tip.last_block_hash,
            ..tx
        })
        .sign(&signer);
        let tx_hash = signed_tx.get_hash();
        assert_eq!(
            env.tx_request_handlers[0].process_tx(signed_tx, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..epoch_length {
            let block = env.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
        }
        let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    }
}
