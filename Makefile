PYTHON ?= python3
SNAPSHOT_DIR ?= data/giro_snapshot_latest
MSG ?= update giro snapshot

.PHONY: giro-snapshot giro-run giro-publish

giro-snapshot:
	$(PYTHON) -m giro.snapshot --out $(SNAPSHOT_DIR)

giro-run:
	streamlit run streamlit_app.py

giro-publish:
	git add Makefile pyproject.toml streamlit_app.py giro $(SNAPSHOT_DIR)
	git commit -m "$(MSG)"
	git push
