import json
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from sqlalchemy import text, create_engine


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
    for tx_hash in tqdm(
        hash_df["transaction_hash"].values, desc=f"Processing block {block_height}"
    ):
        tx_op_df = get_opcode_gas_for_tx(tx_hash, erigon_user, erigon_pass)
        op_df = pd.concat([op_df, tx_op_df], ignore_index=True)
    op_df["block_height"] = block_height
    return op_df


def get_opcode_gas_for_tx(
    tx_hash: str, erigon_user: str, erigon_pass: str, response_max_size: int = 1e10
) -> pd.DataFrame:
    empty_df = pd.DataFrame(
        {"op": [str()], "gas_cost": [np.nan], "count": [np.nan], "tx_hash": [tx_hash]}
    )
    response_str = post_trace_request(
        tx_hash, erigon_user, erigon_pass, response_max_size
    )
    # Check if trace request was sucessfull
    if len(response_str) == 0:
        return empty_df  # Return empty DataFrame
    # Try to convert response string to json
    try:
        data = json.loads(response_str)
    except json.JSONDecodeError:
        print(f"-Transaction trace failed: {tx_hash}")
        print("--Response content is not valid JSON.")
        return empty_df  # Return empty DataFrame
    # Check if the JSON-RPC call was successful
    if "error" in data:
        error = data["error"]
        print(f"-Transaction trace failed: {tx_hash}")
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
        print(f"-Transaction trace failed: {tx_hash}")
        print(f"--An error occurred while making the request: {e}")
        return str()
    if response.status_code != 200:
        print(f"-Transaction trace failed: {tx_hash}")
        print(
            f"--Request failed with status code {response.status_code}: {response.text}"
        )
        return str()
    size = 0
    response_str = str()
    for chunk in response.iter_content(1024):
        size += len(chunk)
        if size > response_max_size:
            print(f"-Transaction trace failed: {tx_hash}")
            print("--Memory usage exceeded")
            return str()
        response_str += chunk.decode("utf-8")
    return response_str


if __name__ == "__main__":
    import os

    with open(os.path.join("secrets.json"), "r") as file:
        secrets_dict = json.load(file)

    fast_tx_hash = "0xe6bfa9f831f20ae8c813aae2222197db27b8f081b8e303247d8923f60db39c40"
    slow_tx_hash = "0xd1c193ec07f6bba8e814431d4f6caaf02ac98707ef99c42c77260996665f2ea1"

    df = get_opcode_gas_for_tx(
        fast_tx_hash, secrets_dict["erigon_username"], secrets_dict["erigon_password"]
    )
    df = get_opcode_gas_for_tx(
        slow_tx_hash, secrets_dict["erigon_username"], secrets_dict["erigon_password"]
    )
