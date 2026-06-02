.PHONY: demo test app clean

demo:            ## Run the chaos demo (zero credentials, stdlib only)
	python demo.py

test:            ## Run the invariant test suite
	python -m pytest -q

app:             ## Run the optional live FastAPI receiver
	uvicorn app:app --reload

clean:
	rm -f webhook.db *.db-wal *.db-shm
	rm -rf .pytest_cache **/__pycache__
