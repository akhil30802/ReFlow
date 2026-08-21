.PHONY: kafka-dev kafka-cluster kafka-down kafka-logs config

kafka-dev:
	docker compose --profile dev up -d

kafka-cluster:
	docker compose --profile cluster up -d

kafka-down:
	docker compose --profile dev --profile cluster down

kafka-logs:
	docker compose --profile dev --profile cluster logs -f

config:
	docker compose --profile cluster config
