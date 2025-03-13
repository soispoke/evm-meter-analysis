import json
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from sqlalchemy import text, create_engine
import concurrent.futures
import os

# Define global variables for parallelism levels
TRANSACTION_THREAD_POOL_SIZE = 40


def get_opcode_gas_for_block(
    block_height: int,
    xatu_user: str,
    xatu_pass: str,
    erigon_user: str,
    erigon_pass: str,
) -> pd.DataFrame:
    query = text(
        """
        SELECT transaction_hash 
        FROM default.canonical_execution_transaction
        WHERE block_number BETWEEN toUInt64(:start_block) AND toUInt64(:end_block)
              AND meta_network_name = :network
        ORDER BY block_number ASC, transaction_index ASC
    """
    )
    db_url = f"clickhouse+http://{xatu_user}:{xatu_pass}@clickhouse.xatu.ethpandaops.io:443/default?protocol=https"
    engine = create_engine(db_url)
    connection = engine.connect()
    query_result = connection.execute(
        query,
        {"start_block": block_height, "end_block": block_height, "network": "mainnet"},
    )
    hash_df = pd.DataFrame(query_result.fetchall())
    op_df = pd.DataFrame()
    tx_hashes = hash_df["transaction_hash"].values
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=TRANSACTION_THREAD_POOL_SIZE
    ) as executor:  # ThreadPoolExecutor is used for transactions within a block as fetching traces is I/O-bound.
        futures = [
            executor.submit(
                get_opcode_gas_for_tx, tx_hash, erigon_user, erigon_pass, block_height
            )
            for tx_hash in tx_hashes
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc=f"Processing block {block_height}",
            position=1,  # Use position=1 for transaction tqdm
            leave=False,  # Keep the transaction tqdm bar after completion
        ):
            tx_op_df = future.result()
            op_df = pd.concat([op_df, tx_op_df], ignore_index=True)
    op_df["block_height"] = block_height
    return op_df


def get_opcode_gas_for_tx(
    tx_hash: str,
    erigon_user: str,
    erigon_pass: str,
    block_height: int,
    response_max_size: int = 1e10,
) -> pd.DataFrame:
    empty_df = pd.DataFrame(
        {"op": [str()], "gas_cost": [np.nan], "count": [np.nan], "tx_hash": [tx_hash]}
    )
    response_str = post_trace_request(
        tx_hash, erigon_user, erigon_pass, response_max_size, block_height
    )
    # Check if trace request was sucessfull
    if len(response_str) == 0:
        return empty_df  # Return empty DataFrame
    # Try to convert response string to json
    try:
        data = json.loads(response_str)
    except json.JSONDecodeError:
        print(f"-Transaction trace failed for block {block_height}: {tx_hash}")
        print("--Response content is not valid JSON.")
        return empty_df  # Return empty DataFrame
    # Check if the JSON-RPC call was successful
    if "error" in data:
        error = data["error"]
        print(f"-Transaction trace failed for block {block_height}: {tx_hash}")
        print(f"--JSON-RPC Error {error.get('code')}: {error.get('message')}")
        return empty_df  # Return empty DataFrame
    # Extract structLogs
    struct_logs = data.get("result", {}).get("structLogs", [])
    if not struct_logs:
        return empty_df  # Return empty DataFrame
    # Convert structLogs to DataFrame
    df = pd.DataFrame(struct_logs)
    # Count repeated rows for memory efficiency and format final dataframe
    df = df.groupby(["op", "gasCost"]).size().reset_index()
    df.columns = ["op", "gas_cost", "count"]
    df["tx_hash"] = tx_hash
    return df


def post_trace_request(
    tx_hash: str,
    erigon_user: str,
    erigon_pass: str,
    response_max_size: int,
    block_height: int,
) -> str:
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, {}],
        "id": 1,
    }
    try:
        response = requests.post(
            "https://rpc-mainnet-teku-erigon-001.utility.production.platform.ethpandaops.io",
            auth=(erigon_user, erigon_pass),
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            stream=True,
        )
    except requests.exceptions.RequestException as e:
        print(f"-Transaction trace failed for block {block_height}: {tx_hash}")
        print(f"--An error occurred while making the request: {e}")
        return str()
    if response.status_code != 200:
        print(f"-Transaction trace failed for block {block_height}: {tx_hash}")
        print(
            f"--Request failed with status code {response.status_code}: {response.text}"
        )
        return str()
    size = 0
    response_str = str()
    for chunk in response.iter_content(1024):
        size += len(chunk)
        if size > response_max_size:
            print(f"-Transaction trace failed for block {block_height}: {tx_hash}")
            print("--Memory usage exceeded")
            return str()
        response_str += chunk.decode("utf-8")
    return response_str


def main():
    # Directories
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(repo_dir, "data")

    # Secrets for acessing xatu clickhouse and erigon
    with open(os.path.join(repo_dir, "secrets.json"), "r") as file:
        secrets_dict = json.load(file)

    # Block heights
    block_start = 22000000  # Mar-08-2025
    block_count = 6000  # ~1 day of ETH blocks

    df = pd.DataFrame()
    for block_height in range(block_start, block_start + block_count):
        block_df = get_opcode_gas_for_block(
            block_height,
            secrets_dict["xatu_username"],
            secrets_dict["xatu_password"],
            secrets_dict["erigon_username"],
            secrets_dict["erigon_password"],
        )
        df = pd.concat([df, block_df], ignore_index=True)
        if block_height % 1000 == 999:  # save and reset
            out_file = os.path.join(
                data_dir, f"opcode_gas_usage_{block_height-999}_{block_height}.csv"
            )
            df.to_csv(out_file, index=False)
            df = pd.DataFrame()
        elif block_height % 50 == 0:  # checkpoint
            temp_file = os.path.join(data_dir, f"opcode_gas_usage_temp.csv")
            df.to_csv(temp_file, index=False)

    # Save last loop, if block counts is not divisible by 1000
    last_block_height = block_start + block_count - 1
    last_mod = last_block_height % 1000
    if last_mod != 999:
        out_file = os.path.join(
            data_dir,
            f"opcode_gas_usage_{block_height-last_mod}_{last_block_height}.csv",
        )
        df.to_csv(out_file, index=False)


if __name__ == "__main__":
    main()
