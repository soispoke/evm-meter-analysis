import sys
import pandas as pd
from typing import Dict, List
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
import opcode_types


def compute_resource_gas_cost_per_tx(
    agg_trace_df: pd.DataFrame,
    tx_gas_info_df: pd.DataFrame,
    comp_df: pd.DataFrame,
    ignore_txs: List[str],
) -> pd.DataFrame:
    # Exclude transactions to ignore
    filtered_agg_trace_df = agg_trace_df[~agg_trace_df["tx_hash"].isin(ignore_txs)]
    filtered_comp_df = comp_df[~comp_df["tx_hash"].isin(ignore_txs)]
    filtered_tx_gas_info_df = tx_gas_info_df[
        ~tx_gas_info_df["tx_hash"].isin(ignore_txs)
    ]
    # Assign opcode costs to resources
    opcode_df = compute_opcode_gas_by_resource(filtered_agg_trace_df)
    # Assign intrinsic base costs to resources
    intrinsic_base_df = compute_intrinsic_base_gas_by_resource(filtered_comp_df)
    # Assign input data costs to resources
    input_data_df = compute_input_data_gas_by_resource(filtered_tx_gas_info_df)
    # Assign refunds to "state" (and make negative) and intrinsic access to "access"
    refunds_and_access_df = filtered_comp_df[
        ["tx_hash", "gas_refund", "intrinsic_access_cost"]
    ].copy()
    refunds_and_access_df = refunds_and_access_df.rename(
        columns={"gas_refund": "State", "intrinsic_access_cost": "Access"}
    )
    refunds_and_access_df["State"] = -refunds_and_access_df["State"]
    # Ignored transactions
    ignored_df = comp_df[comp_df["tx_hash"].isin(ignore_txs)][
        ["tx_hash", "total_gas_cost"]
    ].copy()
    ignored_df = ignored_df.rename(columns={"total_gas_cost": "Unassigned"})
    # Join dataframes
    df = pd.concat(
        [
            opcode_df,
            intrinsic_base_df,
            input_data_df,
            refunds_and_access_df,
            ignored_df,
        ],
        ignore_index=True,
    )
    # Sum all by transaction
    df = df.groupby("tx_hash").sum().reset_index()
    # Add block height and State excluding refunds
    df = df.merge(
        filtered_comp_df[["block_height", "tx_hash", "gas_refund"]],
        on="tx_hash",
        how="left",
    )
    df["State (exc. Refunds)"] = df["State"] + df["gas_refund"]
    df = df.drop(columns="gas_refund")
    return df


def compute_opcode_gas_by_resource(agg_trace_df: pd.DataFrame) -> pd.DataFrame:
    # Apply opcode resource split rule to each row
    split_list = agg_trace_df.apply(
        lambda x: split_opcode_gas_by_resource(
            x["op"],
            x["op_gas_cost"],
            x["op_gas_pair_count"],
            x["post_memory_size"],
            x["memory_expansion"],
            x["tx_hash"],
        ),
        axis=1,
    ).tolist()
    # Aggregate by tx_hash and sum up the resources
    split_df = (
        pd.DataFrame(split_list)
        .drop(columns=["op"])
        .groupby("tx_hash")
        .sum()
        .reset_index()
    )
    return split_df


def compute_intrinsic_base_gas_by_resource(comp_df: pd.DataFrame) -> pd.DataFrame:
    # Apply base intrinsic resource split rule to each row
    split_list = comp_df.apply(
        lambda x: split_intrinsic_base_gas_by_resource(
            x["intrinsic_base_cost"], x["tx_hash"]
        ),
        axis=1,
    ).tolist()
    # Transform to DataFrame
    split_df = pd.DataFrame(split_list)
    return split_df


def compute_input_data_gas_by_resource(tx_gas_info_df: pd.DataFrame) -> pd.DataFrame:
    # Apply base intrinsic resource split rule to each row
    split_list = tx_gas_info_df.apply(
        lambda x: split_input_data_gas_by_resource(
            x["tx_input_zero_bytes"], x["tx_input_nonzero_bytes"], x["tx_hash"]
        ),
        axis=1,
    ).tolist()
    # Transform to DataFrame
    split_df = pd.DataFrame(split_list)
    return split_df


def split_opcode_gas_by_resource(
    op: str,
    gas_cost: float,
    op_count: int,
    post_memory_size: int = None,
    expansion_size: int = None,
    tx_hash: str = None,
) -> Dict[str, float]:
    # Compute memory expansion cost
    if expansion_size > 0:
        memory_expansion_cost = compute_memory_expansion_cost(
            post_memory_size, expansion_size
        )
    else:
        memory_expansion_cost = 0
    # Assign cost for opcodes
    if gas_cost == 0.0:  # case when there is an out of gas error and cost is zero
        resource_dict = {"Compute": 0.0}
    else:
        if op in opcode_types.COMPUTE:
            resource_dict = {"Compute": gas_cost}
        elif op in opcode_types.ACCESS:
            resource_dict = {"Compute": 100.0, "Access": gas_cost - 100.0}
        elif op in opcode_types.MEMORY:
            resource_dict = {
                "Compute": gas_cost - memory_expansion_cost,
                "Memory": memory_expansion_cost,
            }
        elif op in opcode_types.CREATE:
            resource_dict = {
                "Compute": 1000.0,
                "History": 6700.0,
                "State": gas_cost - 7950.0 - memory_expansion_cost,
                "Access": 250.0,
                "Memory": memory_expansion_cost,
            }
        elif (op in opcode_types.EXTCODECOPY) or (op in opcode_types.CALLS):
            resource_dict = {
                "Compute": 100.0,
                "Access": gas_cost - 100.0 - memory_expansion_cost,
                "Memory": memory_expansion_cost,
            }
        elif op in opcode_types.LOG:
            topics = int(op[-1])
            resource_dict = {
                "Compute": 20.0,
                "History": gas_cost - 20.0 - 250.0 * topics - memory_expansion_cost,
                "Bloom topics": 250.0 * topics,
                "Memory": memory_expansion_cost,
            }
        elif op in opcode_types.SELFDESTRUCT:
            if gas_cost > 25000.0:  # funds are sent to empty address
                resource_dict = {
                    "Compute": 5000.0,
                    "State": 25000.0,
                    "Access": gas_cost - 30000.0,
                }
            else:  # funds are sent to non-empty address
                resource_dict = {
                    "Compute": 5000.0,
                    "State": 0.0,
                    "Access": gas_cost - 5000.0,
                }
        elif op in opcode_types.SSTORE:
            if float(gas_cost) in [22100.0, 5000.0, 2200.0]:  # cold access
                resource_dict = {
                    "Compute": 100.0,
                    "State": gas_cost - 2200.0,
                    "Access": 2100.0,
                }
            else:  # warm access
                resource_dict = {
                    "Compute": 100.0,
                    "State": gas_cost - 100.0,
                    "Access": 0.0,
                }
        else:
            resource_dict = {"Unassigned": gas_cost}
    resource_dict = dict((k, op_count * v) for k, v in resource_dict.items())
    resource_dict["op"] = op
    if tx_hash is not None:
        resource_dict["tx_hash"] = tx_hash
    return resource_dict


def split_intrinsic_base_gas_by_resource(
    gas_cost: float, tx_hash: str = None
) -> Dict[str, float]:
    if gas_cost == 21000.0:
        resource_dict = {
            "Compute": 8500.0,
            "History": 6500.0,
            "Access": 300.0,
            "Bandwidth": 5700.0,
        }
    else:
        resource_dict = {
            "Compute": 8500.0 + 1000.0,
            "History": 6500 + 6700.0,
            "State": 21800.0,
            "Access": 300.0 + 2500.0,
            "Bandwidth": 5700.0,
        }
    if tx_hash is not None:
        resource_dict["tx_hash"] = tx_hash
    return resource_dict


def split_input_data_gas_by_resource(
    zero_bytes: float, non_zero_bytes: float, tx_hash: str = None
) -> Dict[str, float]:
    resource_dict = {
        "History": zero_bytes * 0.5 + non_zero_bytes * 5.0,
        "Bandwidth": zero_bytes * 3.5 + non_zero_bytes * 11.0,
    }
    if tx_hash is not None:
        resource_dict["tx_hash"] = tx_hash
    return resource_dict


def compute_memory_expansion_cost(post_memory_size: int, expansion_size: int) -> int:
    # Pre costs
    pre_memory_size = post_memory_size - expansion_size
    pre_memory_size_words = (pre_memory_size + 31) // 32
    pre_cost = ((pre_memory_size_words**2) // 512) + (3 * pre_memory_size_words)
    # Post costs
    post_memory_size_words = (post_memory_size + 31) // 32
    post_cost = ((post_memory_size_words**2) // 512) + (3 * post_memory_size_words)
    # Expansion cost
    expansion_cost = post_cost - pre_cost
    return expansion_cost
