import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path


project_root = str(Path(__file__).parent.parent.parent)
src_dir = os.path.join(project_root, "src")
sys.path.insert(0, src_dir)

from data.opcodes_agg_v2 import compute_gas_costs_for_single_tx


def test_no_depth_change_call():
    # Test case where transaction has a CALL opcode without a depth change
    input_data = {
        "tx_hash": ["0x123"] * 4,
        "op": ["PUSH1", "CALL", "PUSH1", "STOP"],
        "depth": [1] * 4,
        "file_row_number": np.arange(4),
        "gas": [186749, 186746, 177446, 177443],
        "gasCost": [3, 11600, 3, 0],
    }
    input_df = pd.DataFrame(input_data)
    # Expected result
    expected_data = dict(input_data)
    expected_data["op_gas_cost"] = [3, 9300, 3, 0]
    expected_df = pd.DataFrame(expected_data)
    # Actual result
    result_df = compute_gas_costs_for_single_tx(input_df)
    pd.testing.assert_frame_equal(
        result_df, expected_df, check_like=False, check_dtype=False
    )


def test_depth_change_call():
    # Test case where transaction has a CALL opcode with a depth change
    input_data = {
        "tx_hash": ["0x123"] * 6,
        "op": ["GAS", "STATICCALL", "PUSH1", "SWAP1", "RETURN", "ISZERO"],
        "depth": [1, 1, 2, 2, 2, 1],
        "file_row_number": np.arange(6),
        "gas": [50076, 50074, 49194, 48573, 48570, 49350],
        "gasCost": [2, 49294, 3, 3, 0, 3],
    }
    input_df = pd.DataFrame(input_data)
    # Expected result
    expected_data = dict(input_data)
    expected_data["op_gas_cost"] = [2, 100, 3, 3, 0, 3]
    expected_df = pd.DataFrame(expected_data)
    # Actual result
    result_df = compute_gas_costs_for_single_tx(input_df)
    pd.testing.assert_frame_equal(
        result_df, expected_df, check_like=False, check_dtype=False
    )


def test_nested_calls():
    # Test case where transaction has 2 nested CALL opcodes with a depth change
    input_data = {
        "tx_hash": ["0x123"] * 9,
        "op": [
            "GAS",
            "STATICCALL",
            "SWAP1",
            "CALL",
            "GAS",
            "RETURN",
            "PUSH1",
            "RETURN",
            "ISZERO",
        ],
        "depth": [1, 1, 2, 2, 3, 3, 2, 2, 1],
        "file_row_number": np.arange(9),
        "gas": [1000, 998, 500, 497, 200, 198, 395, 392, 790],
        "gasCost": [2, 600, 3, 300, 2, 0, 3, 0, 3],
    }
    input_df = pd.DataFrame(input_data)
    # Expected result
    expected_data = dict(input_data)
    expected_data["op_gas_cost"] = [2, 100, 3, 100, 2, 0, 3, 0, 3]
    expected_df = pd.DataFrame(expected_data)
    # Actual result
    result_df = compute_gas_costs_for_single_tx(input_df)
    print(result_df)
    print(expected_df)
    pd.testing.assert_frame_equal(
        result_df, expected_df, check_like=False, check_dtype=False
    )


def test_sequence_calls():
    # Test case where transaction has 2 CALL opcodes with a depth change in a sequence
    input_data = {
        "tx_hash": ["0x123"] * 9,
        "op": [
            "GAS",
            "STATICCALL",
            "SWAP1",
            "RETURN",
            "GAS",
            "CALL",
            "PUSH1",
            "RETURN",
            "ISZERO",
        ],
        "depth": [1, 1, 2, 2, 1, 1, 2, 2, 1],
        "file_row_number": np.arange(9),
        "gas": [1000, 998, 500, 497, 895, 897, 500, 497, 794],
        "gasCost": [2, 600, 3, 0, 2, 600, 3, 0, 3],
    }
    input_df = pd.DataFrame(input_data)
    # Expected result
    expected_data = dict(input_data)
    expected_data["op_gas_cost"] = [2, 100, 3, 0, 2, 100, 3, 0, 3]
    expected_df = pd.DataFrame(expected_data)
    # Actual result
    result_df = compute_gas_costs_for_single_tx(input_df)
    pd.testing.assert_frame_equal(
        result_df, expected_df, check_like=False, check_dtype=False
    )


def test_failed_transaction():
    # Test case where transaction failed from not enough available gas
    input_data = {
        "tx_hash": ["0x123"] * 4,
        "op": [
            "SWAP1",
            "SSTORE",
            "PUSH1",
            "REVERT",
        ],
        "depth": [1] * 4,
        "file_row_number": np.arange(4),
        "gas": [4991, 4988, 260, 257],
        "gasCost": [3, 20000, 3, 0],
    }
    input_df = pd.DataFrame(input_data)
    # Expected result
    expected_data = dict(input_data)
    expected_data["op_gas_cost"] = [3, 4728, 3, 0]
    expected_df = pd.DataFrame(expected_data)
    # Actual result
    result_df = compute_gas_costs_for_single_tx(input_df)
    pd.testing.assert_frame_equal(
        result_df, expected_df, check_like=False, check_dtype=False
    )
