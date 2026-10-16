use bitcoin;
use bitcoin::hashes::Hash;
use metashrew_core::{flush, get, index_pointer::IndexPointer, input};
use metashrew_support::{compat::export_bytes, index_pointer::KeyValuePointer};
use std::io::Cursor;
use std::sync::Arc;

// Magic prefix bytes for a "protostone" OP_RETURN output. The test indexer
// recognizes any output whose script_pubkey starts with these bytes and runs
// a tiny VM (1 byte opcode + 8 byte alkane id + optional reference outpoint)
// against it. This mimics the alkanes/protorunes wire format closely enough
// to exercise per-outpoint state and per-txid trace semantics during reorgs,
// without pulling in the real alkanes crates.
//
// Layout (after the OP_RETURN + length-prefix push opcodes):
//   [PROTOSTONE_MAGIC (4 bytes)] [opcode (1 byte)] [alkane_id (8 bytes)] [optional ref_outpoint (36 bytes)]
//
// Opcodes:
//   0x01 = MINT     -> write `/outpoint/{txid}:1` = alkane_id, trace = [01, alkane_id]
//   0x02 = BURN     -> delete `/outpoint/{txid}:1`,                  trace = [02, alkane_id]
//   0x03 = TRANSFER -> clear ref_outpoint, write `/outpoint/{txid}:1` = alkane_id,
//                      trace = [03, alkane_id]
//
// The "outpoint" key family models `protorunesbyoutpoint` and the "trace"
// family models `alkanes_trace`. Both are queried by the e2e tests via
// metashrew_view-style accessors below (`get_outpoint`, `get_trace`).
const PROTOSTONE_MAGIC: &[u8] = b"PRTO";

#[cfg(target_arch = "wasm32")]
fn extract_protostone_payload(script: &[u8]) -> Option<&[u8]> {
    // Bitcoin push semantics: OP_RETURN (0x6a), push opcode (length byte 1..=75
    // means "push that many bytes"), then the payload bytes.
    if script.len() < 2 || script[0] != 0x6a {
        return None;
    }
    let push_len = script[1] as usize;
    if push_len == 0 || push_len > 75 {
        return None;
    }
    if script.len() < 2 + push_len {
        return None;
    }
    let payload = &script[2..2 + push_len];
    if payload.len() < PROTOSTONE_MAGIC.len() + 1 + 8 {
        return None;
    }
    if &payload[..PROTOSTONE_MAGIC.len()] != PROTOSTONE_MAGIC {
        return None;
    }
    Some(&payload[PROTOSTONE_MAGIC.len()..])
}

#[cfg(target_arch = "wasm32")]
fn write_outpoint(txid: &bitcoin::Txid, vout: u32, value: &[u8]) {
    let mut pointer = IndexPointer::from_keyword(&format!(
        "/outpoint/{}:{}",
        hex::encode(txid.as_byte_array()),
        vout
    ));
    pointer.set(Arc::new(value.to_vec()));
}

#[cfg(target_arch = "wasm32")]
fn write_trace(txid: &bitcoin::Txid, trace: &[u8]) {
    let mut pointer =
        IndexPointer::from_keyword(&format!("/trace/{}", hex::encode(txid.as_byte_array())));
    pointer.set(Arc::new(trace.to_vec()));
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub fn _start() {
    let mut input_data = Cursor::new(input());
    let height = metashrew_support::utils::consume_sized_int::<u32>(&mut input_data).unwrap();
    let block_bytes = metashrew_support::utils::consume_to_end(&mut input_data).unwrap();

    // Store block data under two keys for testing reorg rollback:
    // 1. /blocks/{height} - the block bytes (may be rolled back during reorg)
    // 2. /block-hashes/{height} - just the hash for quick comparison
    let mut block_pointer = IndexPointer::from_keyword(format!("/blocks/{}", height).as_str());

    block_pointer.set(Arc::new(block_bytes.clone()));
    let block =
        metashrew_support::utils::consensus_decode::<bitcoin::Block>(&mut Cursor::new(block_bytes))
            .unwrap();

    // Also store just the block hash for verification
    let mut hash_pointer = IndexPointer::from_keyword(format!("/block-hashes/{}", height).as_str());
    hash_pointer.set(Arc::new(block.block_hash().as_byte_array().to_vec()));

    let mut tracker = IndexPointer::from_keyword("/blocktracker");
    let mut new_tracker = tracker.get().as_ref().clone();
    new_tracker.extend((&[block.header.block_hash()[0]]).to_vec());
    tracker.set(Arc::new(new_tracker));

    // Walk transactions, looking for protostone OP_RETURN payloads. Side-effects
    // land in /outpoint/* and /trace/* — both append-only via the SMT layer, so
    // a reorg rollback that fails to compact the append-only chain will surface
    // immediately as stale state at the wrong height.
    //
    // Also maintain per-height tx-order tracking for the same-block reordering
    // test: /txids-at-height/{h} is set to the concatenated txids in mined order.
    let mut txid_order: Vec<u8> = Vec::with_capacity(block.txdata.len() * 32);

    for tx in block.txdata.iter() {
        let txid = tx.compute_txid();
        txid_order.extend_from_slice(txid.as_byte_array());

        for output in tx.output.iter() {
            let script = output.script_pubkey.as_bytes();
            let payload = match extract_protostone_payload(script) {
                Some(p) => p,
                None => continue,
            };

            let opcode = payload[0];
            let alkane_id = &payload[1..9];

            match opcode {
                0x01 => {
                    // MINT -> dust lives at vout 1 of this txid (convention used
                    // throughout the e2e tests, matching the wrap-tx fingerprint).
                    write_outpoint(&txid, 1, alkane_id);
                    let mut trace = vec![0x01];
                    trace.extend_from_slice(alkane_id);
                    write_trace(&txid, &trace);
                }
                0x02 => {
                    // BURN -> zero out vout 1 explicitly with an empty value.
                    write_outpoint(&txid, 1, &[]);
                    let mut trace = vec![0x02];
                    trace.extend_from_slice(alkane_id);
                    write_trace(&txid, &trace);
                }
                0x03 => {
                    // TRANSFER -> first input's outpoint loses its value, vout 1
                    // of this txid gains it. Models a CPFP / chained tx.
                    if let Some(first_in) = tx.input.first() {
                        let prev = first_in.previous_output;
                        write_outpoint(&prev.txid, prev.vout, &[]);
                    }
                    write_outpoint(&txid, 1, alkane_id);
                    let mut trace = vec![0x03];
                    trace.extend_from_slice(alkane_id);
                    write_trace(&txid, &trace);
                }
                _ => {
                    // Unknown opcode — record a zero-length trace so the test
                    // can still detect that the tx was indexed at all.
                    write_trace(&txid, &[]);
                }
            }
        }
    }

    let mut order_pointer = IndexPointer::from_keyword(&format!("/txids-at-height/{}", height));
    order_pointer.set(Arc::new(txid_order));

    flush();
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn getblock() -> i32 {
    let mut height_bytes = Cursor::new(input());
    let height = metashrew_support::utils::consume_sized_int::<u32>(&mut height_bytes).unwrap();
    let key = format!("/blocks/{}", height).into_bytes();
    let block_bytes_arc = get(Arc::new(key));
    let block_bytes: &Vec<u8> = &*block_bytes_arc;
    export_bytes(block_bytes.clone())
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub fn blocktracker() -> i32 {
    export_bytes(
        IndexPointer::from_keyword("/blocktracker")
            .get()
            .as_ref()
            .clone(),
    )
}

/// View function: read the alkane id sitting at a given outpoint.
/// Input layout: [txid (32, internal byte order)][vout (u32 little-endian)]
/// Returns: 8-byte alkane id, or empty if no alkane is there.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub fn get_outpoint() -> i32 {
    let mut cur = Cursor::new(input());
    // First 4 bytes are the height prefix the runtime always prepends.
    let _height = metashrew_support::utils::consume_sized_int::<u32>(&mut cur).unwrap();
    let mut txid_bytes = [0u8; 32];
    {
        use std::io::Read;
        cur.read_exact(&mut txid_bytes).unwrap();
    }
    let vout = metashrew_support::utils::consume_sized_int::<u32>(&mut cur).unwrap();
    let key = format!("/outpoint/{}:{}", hex::encode(&txid_bytes), vout).into_bytes();
    let value = get(Arc::new(key));
    export_bytes(value.as_ref().clone())
}

/// View function: read the trace bytes for a given txid.
/// Input layout: [txid (32, internal byte order)]
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub fn get_trace() -> i32 {
    let mut cur = Cursor::new(input());
    let _height = metashrew_support::utils::consume_sized_int::<u32>(&mut cur).unwrap();
    let mut txid_bytes = [0u8; 32];
    {
        use std::io::Read;
        cur.read_exact(&mut txid_bytes).unwrap();
    }
    let key = format!("/trace/{}", hex::encode(&txid_bytes)).into_bytes();
    let value = get(Arc::new(key));
    export_bytes(value.as_ref().clone())
}

/// View function: read the concatenated txid list for a given height.
/// Input layout: [height (u32 little-endian) — the runtime height prefix]
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub fn get_txids_at_height() -> i32 {
    let mut cur = Cursor::new(input());
    let _runtime_height = metashrew_support::utils::consume_sized_int::<u32>(&mut cur).unwrap();
    let query_height = metashrew_support::utils::consume_sized_int::<u32>(&mut cur).unwrap();
    let key = format!("/txids-at-height/{}", query_height).into_bytes();
    let value = get(Arc::new(key));
    export_bytes(value.as_ref().clone())
}
