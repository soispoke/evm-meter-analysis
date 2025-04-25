import json
from enum import Enum


class MEMORY_ACCESS_SIZE(Enum):
    VARIABLE = "variable"
    FIXED = "fixed"


INSTRUCTIONS = {
    "KECCAK256": {
        "opcode": "20",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "CALLDATACOPY": {
        "opcode": "37",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 2}],
    },
    "CODECOPY": {
        "opcode": "39",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 2}],
    },
    "MLOAD": {
        "opcode": "51",
        "access_size": MEMORY_ACCESS_SIZE.FIXED.value,
        "stack_input_positions": [{"offset": 0}],
        "size": 32,
    },
    "MSTORE": {
        "opcode": "52",
        "access_size": MEMORY_ACCESS_SIZE.FIXED.value,
        "stack_input_positions": [{"offset": 0}],
        "size": 32,
    },
    "MSTORE8": {
        "opcode": "53",
        "access_size": MEMORY_ACCESS_SIZE.FIXED.value,
        "stack_input_positions": [{"offset": 0}],
        "size": 8,
    },
    "EXTCODECOPY": {
        "opcode": "3c",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 1, "size": 3}],
    },
    "RETURNDATACOPY": {
        "opcode": "3e",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 2}],
    },
    "MCOPY": {
        "opcode": "5e",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 2}, {"offset": 1, "size": 2}],
    },
    "LOG0": {
        "opcode": "a0",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "LOG1": {
        "opcode": "a1",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "LOG2": {
        "opcode": "a2",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "LOG3": {
        "opcode": "a3",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "LOG4": {
        "opcode": "a4",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "CREATE": {
        "opcode": "f0",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 1, "size": 2}],
    },
    "CALL": {
        "opcode": "f1",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 3, "size": 4}, {"offset": 5, "size": 6}],
    },
    "CALLCODE": {
        "opcode": "f2",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 3, "size": 4}, {"offset": 5, "size": 6}],
    },
    "RETURN": {
        "opcode": "f3",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
    "DELEGATECALL": {
        "opcode": "f4",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 2, "size": 3}, {"offset": 4, "size": 5}],
    },
    "CREATE2": {
        "opcode": "f5",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 1, "size": 2}],
    },
    "STATICCALL": {
        "opcode": "fa",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 2, "size": 3}, {"offset": 4, "size": 5}],
    },
    "REVERT": {
        "opcode": "fd",
        "access_size": MEMORY_ACCESS_SIZE.VARIABLE.value,
        "stack_input_positions": [{"offset": 0, "size": 1}],
    },
}

# Inspired by https://github.com/raxhvl/evm-memory-analysis/blob/main/memory-tracer/tracer/rpc.py
# Learn more at https://geth.ethereum.org/docs/developers/evm-tracing/custom-tracer
CUSTOM_TRACER = f"""
    {{
        data: [],
        required_instructions: {json.dumps(INSTRUCTIONS)},
        fault: function (log) {{}},
        step: function (log) {{
            let name = log.op.toString();
            let refund = log.getRefund();
            if (!refund){{refund = 0}};

            if (Object.keys(this.required_instructions).includes(name)) {{

                const instruction = this.required_instructions[name];

                post_memory_size =  pre_memory_size =  log.memory.length();
                is_fixed_size = instruction.access_size == "{MEMORY_ACCESS_SIZE.FIXED.value}"
                memory_size = 0

                // post_memory_size is the highest offset accessed by the instruction.
                instruction.stack_input_positions.forEach( input =>{{
                    offset = log.stack.peek(input.offset);
                    size = is_fixed_size ? instruction.size :
                           log.stack.peek(input.size);

                    if(size>0){{
                        post_memory_size = Math.max(post_memory_size, offset + size);
                    }}
                    memory_size = memory_size + size;
                }});

                // Ensure post_memory_size is a multiple of 32 (rounded up)
                post_memory_size = Math.ceil(post_memory_size / 32) * 32;

                memory_expansion = post_memory_size - pre_memory_size;

                cum_refund = 0;

                const call_opcodes = ["DELEGATECALL", "STATICCALL", "CALL", "CALLCODE"]
                if(call_opcodes.includes(name)){{
                    call_address = log.stack.peek(1).toString(16);
                }} else {{
                    call_address = "";
                }}

            }} else {{
                if(name=="SSTORE"){{
                    cum_refund = refund;
                }} else {{
                    cum_refund = 0;
                }};

                pre_memory_size = 0;
                post_memory_size = 0;
                memory_expansion = 0;
                memory_size = 0;
                call_address = "";

            }}

            this.data.push({{
                op: name,
                gas: log.getGas(),
                gas_cost: log.getCost(),
                depth: log.getDepth(),
                post_memory_size,
                memory_expansion,
                memory_size,
                cum_refund,
                call_address,
            }});
        }},
        result: function (ctx, db) {{
            return {{
                data: this.data
            }};
        }}
    }}
"""
