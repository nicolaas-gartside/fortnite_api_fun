test:
	python -m pytest
d_start:
	docker compose up --detach
d_stop:
	docker compose stop