use {
    crate::bigtable::RowKey,
    serde::Serialize,
    serde_json::{json, Value},
    // crate::{
    //     message::Message,
    // },
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        instruction::CompiledInstruction,
        message::VersionedMessage,
        pubkey::Pubkey,
        signature::Signature,
        transaction::Transaction,
        transaction::VersionedTransaction,
    },
    solana_sdk::{
        deserialize_utils::default_on_eof, message::v0::LoadedAddresses, sysvar::is_sysvar_id,
    },
    solana_storage_bigtable::{
        bigtable, key_to_slot, slot_to_tx_by_addr_key, LedgerStorage, LegacyTransactionByAddrInfo,
        Result, TransactionInfo, Error, slot_to_blocks_key, StoredConfirmedBlock,
    },
    solana_storage_proto::convert::{generated, tx_by_addr},

    solana_transaction_status::TransactionByAddrInfo,
    solana_transaction_status::{
        extract_and_fmt_memos, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta, Reward, TransactionConfirmationStatus,
        TransactionStatus, TransactionStatusMeta, VersionedConfirmedBlock,
        VersionedTransactionWithStatusMeta,
    },
    solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
    std::{
        collections::{HashMap, HashSet},
        convert::TryInto,
    },
    std::{env, str::FromStr},
};

const SERUM_DEX_V3_ADDRESS: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";

type Index = u32;

#[derive(Serialize)]
enum SerializableTransaction {
    Regular(Transaction),
    Versioned(VersionedTransaction),
}

// async fn print_transactions(
//     starting_slot: Slot,
//     limit: usize,
//     config: solana_storage_bigtable::LedgerStorageConfig,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
//         .await
//         .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

//     let range = (starting_slot..starting_slot + (limit as u64)).collect::<Vec<Slot>>();
//     let slots: &[Slot] = range.as_slice().try_into().unwrap();

//     // println!("Starting fetching from BigTable");

//     let blocks_with_data: Vec<(Slot, ConfirmedBlock)> = bigtable
//         .get_confirmed_blocks_with_data(slots)
//         .await?
//         .collect();

//     // println!("Finished fetching from BigTable");

//     // println!("Starting extracting transactions");

//     let transactions: Vec<TransactionWithStatusMeta> = blocks_with_data.iter().map(|(_, block)| {
//         block.transactions.clone()
//     }).flatten().into_iter().collect();

//     // println!("Finished extracting transactions");

//     // println!("Started processing transactions");

//     let processed_transactions: Vec<String> = transactions.into_iter()
//     .filter(|txn_wrapper| {
//         let (instructions, account_keys): (&Vec<CompiledInstruction>, &Vec<Pubkey>) = match txn_wrapper {
//             TransactionWithStatusMeta::MissingMetadata(txn) => (&txn.message.instructions, &txn.message.account_keys),
//             TransactionWithStatusMeta::Complete(txn_with_meta) => {
//                 let message_type = &txn_with_meta.transaction.message;
//                 match message_type {
//                     VersionedMessage::Legacy(message) => (&message.instructions, &message.account_keys),
//                     VersionedMessage::V0(message) => (&message.instructions, &message.account_keys),
//                 }
//             }
//         };
//         account_keys.contains(&Pubkey::from_str(SERUM_DEX_V3_ADDRESS).unwrap())
//     })
//     .enumerate()
//     .map(|(i, txn_wrapper)| {
//         // if i % 100 == 0 {
//         //     println!("{}", i);
//         // }
//         // match txn_wrapper {
//         //     TransactionWithStatusMeta::MissingMetadata(txn) => SerializableTransaction::Regular(txn),
//         //     TransactionWithStatusMeta::Complete(txn_with_meta) => SerializableTransaction::Versioned(txn_with_meta.transaction),
//         // }
//         match txn_wrapper {
//             TransactionWithStatusMeta::MissingMetadata(txn) => txn.message.account_keys[0].to_string(),
//             TransactionWithStatusMeta::Complete(txn_with_meta) => {
//                 let message_type = txn_with_meta.transaction.message;
//                 match message_type {
//                     VersionedMessage::Legacy(message) => message.account_keys[0].to_string(),
//                     VersionedMessage::V0(message) => message.account_keys[0].to_string(),
//                 }
//             },
//         }
//     })
//     .collect();

//     print!("{}", json!(processed_transactions));

//     Ok(())
// }


// async fn get_blocks_for_signatures<'a>(signatures: &'a [Signature]) -> Result<Vec<Slot>> {
//     let mut bigtable = create_bigtable_connection().await?.connection.client();
//     let signatures_as_string: Vec<String> = signatures.iter().map(|signature| signature.to_string()).collect();
//     let transaction_infos = bigtable
//         .get_bincode_cells::<TransactionInfo>(
//             "tx",
//             &signatures_as_string,
//         )
//         .await
//         .map_err(|err| match err {
//             bigtable::Error::RowNotFound => Error::SignatureNotFound,
//             _ => err.into(),
//         })?;
//     let slots = transaction_infos
//         .into_iter()
//         .map(|(_, transaction_info)| transaction_info.expect("Transaction info should exist").slot)
//         .collect();
//     Ok(slots)
// }


async fn create_bigtable_connection() -> Result<solana_storage_bigtable::LedgerStorage> {
    let config = solana_storage_bigtable::LedgerStorageConfig::default();
    Ok(
        solana_storage_bigtable::LedgerStorage::new_with_config(config)
            .await
            .map_err(|err| format!("Failed to connect to storage: {:?}", err))
            .unwrap(),
    )
}

fn timestamp_to_slot(slot: Slot) -> UnixTimestamp {
    unimplemented!();
}

async fn fetch_signatures_for_address(
    address: &Pubkey,
    start_slot: Slot,
    end_slot: Slot,
) -> Result<Vec<(Signature, Slot, UnixTimestamp, Index)>> {
    let mut bigtable = create_bigtable_connection().await?.connection.client();
    let address_prefix = format!("{}/", address);

    let mut infos = vec![];

    let tx_by_addr_data = bigtable
        .get_row_data(
            "tx-by-addr",
            Some(format!(
                "{}{}",
                address_prefix,
                slot_to_tx_by_addr_key(start_slot),
            )),
            Some(format!(
                "{}{}",
                address_prefix,
                slot_to_tx_by_addr_key(end_slot),
            )),
            0,
        )
        .await?;

    'outer: for (row_key, data) in tx_by_addr_data {
        let slot = !key_to_slot(&row_key[address_prefix.len()..]).ok_or_else(|| {
            bigtable::Error::ObjectCorrupt(format!(
                "Failed to convert key to slot: tx-by-addr/{}",
                row_key
            ))
        })?;

        let deserialized_cell_data = bigtable::deserialize_protobuf_or_bincode_cell_data::<
            Vec<LegacyTransactionByAddrInfo>,
            tx_by_addr::TransactionByAddr,
        >(&data, "tx-by-addr", row_key.clone())?;

        let cell_data: Vec<TransactionByAddrInfo> = match deserialized_cell_data {
            bigtable::CellData::Bincode(tx_by_addr) => {
                tx_by_addr.into_iter().map(|legacy| legacy.into()).collect()
            }
            bigtable::CellData::Protobuf(tx_by_addr) => tx_by_addr.try_into().map_err(|error| {
                bigtable::Error::ObjectCorrupt(format!(
                    "Failed to deserialize: {}: tx-by-addr/{}",
                    error,
                    row_key.clone()
                ))
            })?,
        };

        infos = cell_data
            .into_iter()
            .map(|tx_by_addr_info| {
                (
                    tx_by_addr_info.signature,
                    slot,
                    tx_by_addr_info.block_time.expect("Block time should exist"),
                    tx_by_addr_info.index,
                )
            })
            .collect();
    }
    Ok(infos)
}

async fn fetch_txns_from_blocks<'a>(block_infos: &'a [(Slot, Index)]) -> Result<Vec<SerializableTransaction>> {
    let mut bigtable = create_bigtable_connection().await?.connection.client();
    let mut slot_to_indices: HashMap<Slot, Vec<Index>> = HashMap::new();
    block_infos.into_iter().fold(&mut slot_to_indices, |acc, (slot, index)| {
        match acc.get_mut(slot) {
            Some(indices) => {
                indices.push(*index);
                acc
            },
            None => {
                acc.insert(*slot, vec![*index]);
                acc
            }
        }
    });
    let row_keys = block_infos.iter().copied().map(|(slot, _)| slot_to_blocks_key(slot));
    let data = bigtable
        .get_protobuf_or_bincode_cells("blocks", row_keys)
        .await?
        .filter_map(
            |(row_key, block_cell_data): (
                RowKey,
                bigtable::CellData<StoredConfirmedBlock, generated::ConfirmedBlock>,
            )| {
                let block: ConfirmedBlock = match block_cell_data {
                    bigtable::CellData::Bincode(block) => block.into(),
                    bigtable::CellData::Protobuf(block) => block.try_into().ok()?,
                };
                Some((key_to_slot(&row_key).unwrap(), block))
            },
        );

    let mut all_transactions: Vec<SerializableTransaction> = Vec::new();
    for (slot, block) in data {
        let transactions = slot_to_indices
            .get(&slot)
            .expect("Slot should exist in map")
            .into_iter()
            .map(|index| {
                match block.transactions.get(*index as usize).expect("Txn should exist at index") {
                    // Maybe remove the clone here and make it more efficient?
                    TransactionWithStatusMeta::MissingMetadata(txn) => SerializableTransaction::Regular(txn.clone()),
                    TransactionWithStatusMeta::Complete(txn_with_meta) => SerializableTransaction::Versioned(txn_with_meta.clone().transaction),
            }
        });
        all_transactions.extend(transactions);
    }
    Ok(all_transactions)
}

fn save_txns_to_disk(transactions: Result<Vec<SerializableTransaction>>) {
    unimplemented!();
}

fn save_signatures_to_disk(signature_infos: Vec<(Signature, Slot, UnixTimestamp, Index)>) {
    unimplemented!();
}

async fn cache_txns_for_address(address: &Pubkey, start_slot: Slot, end_slot: Slot, batch_size: u64) {
    let mut current_start_slot = start_slot;
    while current_start_slot <= end_slot {
        let signature_infos = fetch_signatures_for_address(address, current_start_slot, current_start_slot + batch_size).await.unwrap();
        let block_infos: &[(Slot, Index)] = &signature_infos.into_iter().map(|(_, slot, _, index)| (slot, index)).collect::<Vec<(Slot, Index)>>();
        let transactions = fetch_txns_from_blocks(block_infos);
        current_start_slot += batch_size
    }
    todo!();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let starting_slot = args[1].parse::<u64>().unwrap();
    let limit = args[2].parse::<usize>().unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let config = solana_storage_bigtable::LedgerStorageConfig::default();

    // runtime.block_on(print_transactions(starting_slot, limit, config)).unwrap();
}
