setup:
	python -m venv .venv && \
		. .venv/bin/activate && pip install -r requirements.txt

opcodes-raw:
	. .venv/bin/activate && \
		 python src/data/opcodes_raw.py

opcodes-agg:
	. .venv/bin/activate && \
		 python src/data/opcodes_agg.py