import logging
import traceback
import numpy as np
import pandas as pd

PRECOMPILE_MAP = {
    1:  {"name": "ecRecover",        "fixed_cost": 3000.0},
    2:  {"name": "SHA2-256",         "fixed_cost":   60.0},
    3:  {"name": "RIPEMD-160",       "fixed_cost":  600.0},
    4:  {"name": "identity",         "fixed_cost":   15.0},
    5:  {"name": "modexp",           "fixed_cost":  200.0},
    6:  {"name": "ecAdd",            "fixed_cost":  150.0},
    7:  {"name": "ecMul",            "fixed_cost": 6000.0},
    8:  {"name": "ecPairing",        "fixed_cost":45000.0},
    9:  {"name": "blake2f",          "fixed_cost":    0.0},
    10: {"name": "point_evaluation", "fixed_cost":50000.0},
}

PRECOMPILE_NAMES = {info["name"] for info in PRECOMPILE_MAP.values()}

def is_precompile(addr_hex: str) -> bool:
    try:
        return int(addr_hex, 16) in PRECOMPILE_MAP
    except ValueError:
        return False

def map_precompile(addr_hex: str) -> str | None:
    return PRECOMPILE_MAP[int(addr_hex, 16)]["name"] if is_precompile(addr_hex) else None

def map_precompile_fixed_cost(addr_hex: str) -> float:
    return PRECOMPILE_MAP[int(addr_hex, 16)]["fixed_cost"] if is_precompile(addr_hex) else 0.0

# ───────────────────────────────────────────────────────────────
# 2.  Memory‑expansion gas from the yellow‑paper formula
# ───────────────────────────────────────────────────────────────
def _memory_expansion_cost(post_sz: int, expansion: int) -> int:
    if expansion <= 0:
        return 0
    pre_sz  = post_sz - expansion
    w_pre   = (pre_sz  + 31) // 32
    w_post  = (post_sz + 31) // 32
    return (w_post**2 // 512 + 3 * w_post) - (w_pre**2 // 512 + 3 * w_pre)

# ───────────────────────────────────────────────────────────────
# 3.  Constants for the compute‑only heuristic
# ───────────────────────────────────────────────────────────────
CALL_BASE             = 100.0
COLD_ACCESS_SURCHARGE  = 2500.0

# ───────────────────────────────────────────────────────────────
# 4.  Main fixer
# ───────────────────────────────────────────────────────────────
def fix_op_gas_cost_for_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds `compute_gas_cost` for every execution:
      compute = op_gas_cost
                – memory_expansion_cost
                – 100                 (CALL base)
                – 2500 if cold access (heuristic)
    Also renames STATICCALL/CALL rows to their pre‑compile mnemonic.
    """
    out_frames = []

    for tx_hash in df["tx_hash"].unique():
        tx_df = df[df["tx_hash"] == tx_hash]

        # your existing depth‑aware cost fixer
        try:
            fixed_df = fix_op_gas_costs_for_single_tx(tx_df)
        except Exception:
            logging.error(f"Gas‑fix failed for tx {tx_hash}", exc_info=True)
            fixed_df = tx_df.copy()
            fixed_df["op_gas_cost"] = fixed_df["gas_cost"]

        # Rename any pre‑compile target
        fixed_df["op"] = fixed_df["call_address"].apply(map_precompile).fillna(fixed_df["op"])

        # Compute‑only column
        def _compute_cost(row):
            if row["op"] not in PRECOMPILE_NAMES:
                return row["op_gas_cost"]          # ordinary opcode

            mem_cost   = _memory_expansion_cost(row["post_memory_size"], row["memory_expansion"])
            fixed_cost = map_precompile_fixed_cost(row["call_address"])

            # Heuristic: decide cold after subtracting *both* fixed & mem cost
            cold = (row["op_gas_cost"] - CALL_BASE - fixed_cost - mem_cost) >= COLD_ACCESS_SURCHARGE

            return (
                row["op_gas_cost"]
                - mem_cost
                - CALL_BASE
                - (COLD_ACCESS_SURCHARGE if cold else 0.0)
            )

        fixed_df["compute_gas_cost"] = fixed_df.apply(_compute_cost, axis=1)
        out_frames.append(fixed_df)

    return pd.concat(out_frames, ignore_index=True)

def fix_op_gas_costs_for_single_tx(initial_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column named `op_gas_cost` to initial_df with the correct gas cost for the
    CALL-type opcodes and contract creation opcodes. initial_df must have at least the following
    columns: op, gas, gas_cost, depth, file_row_number
    Note that this code assume df only contains data for one transaction.
    """
    df = initial_df.copy()
    if df["depth"].nunique() == 1:  # if transaction does not have depth changes
        # We only need to fix calls
        df["op_gas_cost"] = np.where(
            df["op"].isin(["DELEGATECALL", "STATICCALL", "CALL", "CALLCODE"]),
            df["gas"] - df["gas"].shift(-1),
            df["gas_cost"],
        )
        return df
    else:  # if the transaction has depth changes...
        df["has_depth_increase"] = df["depth"] < df["depth"].shift(-1)
        df["has_depth_decrease"] = df["depth"] > df["depth"].shift(-1)
        df["has_depth_change"] = df["depth"] != df["depth"].shift(-1)
        inner_gas_diff_list = []
        outer_gas_diff_list = []
        change_depth_row_numbers = df[df["has_depth_change"]]["file_row_number"].values
        filter_row_numbers = np.unique(
            np.concat(
                (
                    change_depth_row_numbers,
                    change_depth_row_numbers + 1,
                    change_depth_row_numbers - 1,
                )
            )
        )
        filter_rows_df = df[df["file_row_number"].isin(filter_row_numbers)].sort_values(
            "file_row_number"
        )
        for i in range(len(filter_rows_df) - 1):
            row = filter_rows_df.iloc[i]
            next_row = filter_rows_df.iloc[i + 1]
            # if the row is a new depth starter
            if row["has_depth_increase"]:
                # Get the next row that decreases the depth
                return_row = filter_rows_df[
                    (filter_rows_df["file_row_number"] > row["file_row_number"])
                    & (filter_rows_df["has_depth_decrease"])
                    & (filter_rows_df["depth"] == row["depth"] + 1)
                ].iloc[0]
                # Get and save the available gas at entry and exit
                ## inside the call
                inner_entry_gas = next_row["gas"]
                inner_exit_gas = filter_rows_df[
                    filter_rows_df["file_row_number"] == return_row["file_row_number"]
                ].iloc[0]["gas"]
                inner_gas_diff_list.append(inner_entry_gas - inner_exit_gas)
                ## right outside the call
                outer_entry_gas = row["gas"]
                outer_exit_gas = filter_rows_df[
                    filter_rows_df["file_row_number"]
                    == return_row["file_row_number"] + 1
                ].iloc[0]["gas"]
                outer_gas_diff_list.append(outer_entry_gas - outer_exit_gas)
            else:
                inner_gas_diff_list.append(np.nan)
                outer_gas_diff_list.append(np.nan)
        # Compute the correct cost for the depth starter rows
        depth_starter_cost_df = pd.DataFrame(
            {
                "file_row_number": filter_rows_df["file_row_number"].values,
                "op_gas_cost": np.array(outer_gas_diff_list + [np.nan])
                - np.array(inner_gas_diff_list + [np.nan]),
            }
        )
        # Merge with original df
        df = df.merge(depth_starter_cost_df, on="file_row_number", how="left").drop(
            columns=["has_depth_change", "has_depth_increase", "has_depth_decrease"]
        )
        df["op_gas_cost"] = np.where(
            df["op_gas_cost"].isna(), df["gas_cost"], df["op_gas_cost"]
        )
        # Now, we only need to fix calls of same depth
        df["op_gas_cost"] = np.where(
            (df["op"].isin(["DELEGATECALL", "STATICCALL", "CALL", "CALLCODE"]))
            & (df["depth"] == df["depth"].shift(-1)),
            df["gas"] - df["gas"].shift(-1),
            df["op_gas_cost"],
        )
        return df


def aggregate_op_gas_cost_data(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "block_height",
        "tx_hash",
        "op",
        "post_memory_size",
        "memory_expansion",
        "memory_size",
        "cum_refund",
        "call_address",
    ]
    groupby_cols = [c for c in base_cols if c in df.columns]

    agg_df = (
        df
        .groupby(groupby_cols)
        .agg(
            op_gas_pair_count = ("op_gas_cost",     "size"),   # how many executions
            op_gas_cost_sum   = ("op_gas_cost",     "sum"),    # full EVM charge total
            compute_gas_sum   = ("compute_gas_cost","sum"),    # compute‑only total
        )
        .reset_index()
    )
    return agg_df


def compute_component_gas_costs_per_tx(
    agg_trace_df: pd.DataFrame,
    tx_gas_info_df: pd.DataFrame,
) -> pd.DataFrame:
    # Get total cost and input data cost
    comp_df = tx_gas_info_df[
        ["block_height", "tx_hash", "tx_gas_cost", "tx_input_data_cost"]
    ].copy()
    comp_df = comp_df.rename(
        columns={
            "tx_gas_cost": "total_gas_cost",
            "tx_input_data_cost": "input_data_cost",
        }
    )
    # Get base intrinsic cost
    intrinsic_base_df = get_intrinsic_base_cost_per_tx(tx_gas_info_df)
    comp_df = comp_df.merge(intrinsic_base_df, on="tx_hash", how="outer")
    # Get opcode cost
    opcode_df = get_opcode_gas_cost(agg_trace_df)
    comp_df = comp_df.merge(opcode_df, on="tx_hash", how="outer")
    # Get gas refund
    refund_df = get_gas_refunds_per_tx(agg_trace_df)
    comp_df = comp_df.merge(refund_df, on="tx_hash", how="outer")
    # Fill nan
    comp_df = comp_df.fillna(0.0)
    # Get intrinsic access cost
    # Currently estimating it from the other components...
    # TODO: Fix the estimation function get_intrinsic_access_cost_per_tx
    comp_df["intrinsic_access_cost"] = (
        comp_df["total_gas_cost"]
        - comp_df["intrinsic_base_cost"]
        - comp_df["input_data_cost"]
        - comp_df["op_gas_cost"]
        + comp_df["gas_refund"]
    )
    return comp_df


def get_intrinsic_base_cost_per_tx(tx_gas_info_df: pd.DataFrame) -> pd.DataFrame:
    df = tx_gas_info_df[["tx_hash", "is_contract_creation"]].copy()
    df["intrinsic_base_cost"] = np.where(
        df["is_contract_creation"] == 1, 53000.0, 21000.0
    )
    df = df.drop(columns="is_contract_creation")
    return df


def get_opcode_gas_cost(agg_trace_df: pd.DataFrame) -> pd.DataFrame:
    df = (
        agg_trace_df.groupby("tx_hash")["op_total_gas_cost"]
        .sum()
        .reset_index(name="op_gas_cost")
    )
    return df


def get_intrinsic_access_cost_per_tx(
    tx_gas_info_df: pd.DataFrame,
    avail_gas_df: pd.DataFrame,
) -> pd.DataFrame:
    df = avail_gas_df.merge(tx_gas_info_df, on="tx_hash", how="inner")
    # Check if tx is simple - an ETH transfer or a call data transaction
    df["is_simple_tx"] = df["tx_gas_cost"] == (21000.0 + df["tx_input_data_cost"])
    # if it simple, no intrinsic access cost. if not simple, compute access cost
    df["intrinsic_access_cost"] = np.where(
        df["is_simple_tx"],
        0.0,
        df["tx_gas_limit"] - df["tx_avail_gas"] - df["tx_input_data_cost"] - 21000.0,
    )
    # futher discount base intrinsic cost if the tx is a contraction creation
    df["intrinsic_access_cost"] = np.where(
        df["is_contract_creation"],
        df["intrinsic_access_cost"] - 32000.0,
        df["intrinsic_access_cost"],
    )
    df = df[["tx_hash", "intrinsic_access_cost"]]
    return df


def get_gas_refunds_per_tx(agg_trace_df: pd.DataFrame) -> pd.DataFrame:
    df = (
        agg_trace_df.groupby("tx_hash")["cum_refund"]
        .max()
        .reset_index(name="gas_refund")
    )
    return df
