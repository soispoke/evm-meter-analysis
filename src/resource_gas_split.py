from typing import Dict
import opcode_types


def split_opcode_gas_by_resource(
    op: str, gas_cost: float, op_count: int, tx_hash: str = None
) -> Dict[str, float]:
    if gas_cost == 0.0:  # case when there is an out of gas error and cost is zero
        resource_dict = {"compute": 0.0}
    else:
        if op in opcode_types.COMPUTE:
            resource_dict = {"compute": gas_cost}
        elif op in opcode_types.ACCESS:
            resource_dict = {"compute": 100.0, "access": gas_cost - 100.0}
        elif op in opcode_types.MEMORY_0:
            resource_dict = {"compute": 0.0, "memory": gas_cost}
        elif op in opcode_types.MEMORY_3:
            resource_dict = {"compute": 3.0, "memory": gas_cost - 3.0}
        elif op in opcode_types.MEMORY_30:
            resource_dict = {"compute": 30.0, "memory": gas_cost - 30.0}
        elif op in opcode_types.CREATE:
            resource_dict = {
                "compute": 1000.0,
                "history_growth": 6700.0,
                "state_growth": gas_cost - 7950.0,
                "access": 250.0,
            }
        elif (op in opcode_types.EXTCODECOPY) or (op in opcode_types.CALLS):
            if gas_cost > 2600.0:
                resource_dict = {
                    "compute": 100.0,
                    "access": 2500.0,
                    "memory": gas_cost - 2600.0,
                }
            else:
                resource_dict = {
                    "compute": 100.0,
                    "access": 0.0,
                    "memory": gas_cost - 100.0,
                }
        elif op in opcode_types.LOG:
            topics = int(op[-1])
            resource_dict = {
                "compute": 20.0,
                "history_growth": gas_cost - 20.0 - 250.0 * topics,
                "bloom_topics": 250.0 * topics,
            }
        elif op in opcode_types.SELFDESTRUCT:
            if gas_cost > 25000.0:  # funds are sent to empty address
                resource_dict = {
                    "compute": 5000.0,
                    "state_growth": 25000.0,
                    "access": gas_cost - 30000.0,
                }
            else:  # funds are sent to non-empty address
                resource_dict = {
                    "compute": 5000.0,
                    "state_growth": 0.0,
                    "access": gas_cost - 5000.0,
                }
        elif op in opcode_types.SSTORE:
            if float(gas_cost) in [22100.0, 5000.0, 2200.0]:  # cold access
                resource_dict = {
                    "compute": 100.0,
                    "state_growth": gas_cost - 2200.0,
                    "access": 2100.0,
                }
            else:  # warm access
                resource_dict = {
                    "compute": 100.0,
                    "state_growth": gas_cost - 100.0,
                    "access": 0.0,
                }

        else:
            resource_dict = {"unassigned": gas_cost}
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
            "compute": 4300.0,
            "history_growth": 6700.0,
            "state_growth": 5000.0,
            "access": 5000.0,
        }
    else:
        resource_dict = {
            "compute": 4300.0 + 1000.0,
            "history_growth": 6700.0 * 2,
            "state_growth": 5000.0 + 21800.0,
            "access": 5000.0 + 2500.0,
        }
    if tx_hash is not None:
        resource_dict["tx_hash"] = tx_hash
    return resource_dict
