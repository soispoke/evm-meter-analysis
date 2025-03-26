from typing import Dict
import opcode_types


def split_opcode_gas_by_resource(
    op: str, gas_cost: float, tx_hash: str = None
) -> Dict[str, float]:
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
                "memory": gas_cost,
            }
    elif op in opcode_types.LOG:
        topics = int(op[-1])
        resource_dict = {
            "compute": 20.0,
            "history_growth": gas_cost - 20.0 - 250.0 * topics,
            "bloom_topics": 250.0 * topics,
        }
    else:
        resource_dict = {"unassigned": gas_cost}
    resource_dict["op"] = op
    if tx_hash is not None:
        resource_dict["tx_hash"] = tx_hash
    return resource_dict


def main():
    print(len(opcode_types.COMPUTE))
    return


if __name__ == "__main__":
    main()
