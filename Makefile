PY ?= ./venv/bin/python
APP ?= app
REPORTS ?= reports
DATE ?=
CONFIRM ?= NO
COMPRESS ?= NO
ALLOW_TODAY ?= NO
ENV_FILE ?= .env
DB_PATH ?= data/chain_monitor.sqlite

.PHONY: help env-check compile run run-research health coverage quality-summary quality-csv db-init db-summary db-integrity db-report db-size db-value-audit db-retention db-compact-dry-run db-compact-execute db-vacuum db-maintenance db-migrate-all db-migrate-date sqlite-checkpoint archive-compress-date archive-compress-dry-run archive-status daily-close daily-close-strict db-prune-dry-run db-prune-execute report-overnight report-state report-run report-all smoke test-sqlite test-opportunity test-state test-action test-lp test-core test-all archive-check git-info preflight

help:
	@printf '%s\n' "chain-monitor common targets:"
	@printf '%s\n' "  make help                Show this help."
	@printf '%s\n' "  make env-check           Print safe, non-sensitive runtime config."
	@printf '%s\n' "  make compile             Compile app/ and reports/ with $(PY)."
	@printf '%s\n' "  make run                 Start the main listener."
	@printf '%s\n' "  make run-research        Start with research tier and OKX/Kraken live context."
	@printf '%s\n' "  make health              Show market context health."
	@printf '%s\n' "  make coverage            Show major pool coverage."
	@printf '%s\n' "  make quality-summary     Show LP quality summary."
	@printf '%s\n' "  make quality-csv         Export quality rows to reports/quality_rows.csv."
	@printf '%s\n' "  make db-init             Initialize SQLite schema."
	@printf '%s\n' "  make db-summary          Show SQLite health summary."
	@printf '%s\n' "  make db-integrity        Run SQLite integrity checks."
	@printf '%s\n' "  make db-report           Run SQLite db-summary, db-integrity, opportunity summary."
	@printf '%s\n' "  make db-size             Show SQLite DB/WAL/table/json payload size breakdown."
	@printf '%s\n' "  make db-value-audit      Generate SQLite data-value audit reports."
	@printf '%s\n' "  make db-retention        Show SQLite retention / compact recommendations."
	@printf '%s\n' "  make db-compact-dry-run  Preview compact savings without modifying DB."
	@printf '%s\n' "  make db-compact-execute  Run compact only with CONFIRM=YES."
	@printf '%s\n' "  make db-vacuum           Run VACUUM only with CONFIRM=YES."
	@printf '%s\n' "  make db-maintenance      Run db-size, db-value-audit, db-retention, db-compact-dry-run."
	@printf '%s\n' "  make db-migrate-all      Mirror all archive NDJSON into SQLite."
	@printf '%s\n' "  make db-migrate-date     Mirror one archive date. Usage: make db-migrate-date DATE=YYYY-MM-DD"
	@printf '%s\n' "  make sqlite-checkpoint   Run SQLite WAL checkpoint truncate."
	@printf '%s\n' "  make archive-status      Show archive file status. Usage: make archive-status DATE=YYYY-MM-DD"
	@printf '%s\n' "  make archive-compress-dry-run  Preview gzip for one date. Usage: make archive-compress-dry-run DATE=YYYY-MM-DD"
	@printf '%s\n' "  make archive-compress-date     Gzip one date only with CONFIRM=YES."
	@printf '%s\n' "  make daily-close         Migrate archive, check DB, generate reports, dry-run compression, checkpoint."
	@printf '%s\n' "  make daily-close-strict  Daily close with strict DB/archive mismatch failure."
	@printf '%s\n' "  make db-prune-dry-run    Preview SQLite retention prune."
	@printf '%s\n' "  make db-prune-execute    Execute SQLite prune only with CONFIRM=YES."
	@printf '%s\n' "  make report-overnight    Generate overnight trade action report."
	@printf '%s\n' "  make report-state        Generate afternoon/evening state report."
	@printf '%s\n' "  make report-run          Generate overnight run report."
	@printf '%s\n' "  make report-all          Generate all common reports."
	@printf '%s\n' "  make smoke               Compile and run DB, health, coverage checks."
	@printf '%s\n' "  make test-sqlite         Run SQLite tests."
	@printf '%s\n' "  make test-opportunity    Run trade opportunity tests."
	@printf '%s\n' "  make test-state          Run asset market state tests."
	@printf '%s\n' "  make test-action         Run trade action tests."
	@printf '%s\n' "  make test-lp             Run LP/report/archive tests."
	@printf '%s\n' "  make test-core           Run operational intent and CLMM core tests."
	@printf '%s\n' "  make test-all            Run all grouped tests."
	@printf '%s\n' "  make archive-check       List latest archive directories."
	@printf '%s\n' "  make git-info            Show git status and latest commit."
	@printf '%s\n' "  make preflight           Run env-check, smoke, archive-check, git-info."

env-check:
	@$(PY) -c 'import importlib, json; c = importlib.import_module("$(APP).config"); keys = ("DEFAULT_USER_TIER", "MARKET_CONTEXT_ADAPTER_MODE", "MARKET_CONTEXT_PRIMARY_VENUE", "MARKET_CONTEXT_SECONDARY_VENUE", "ARCHIVE_ENABLE_RAW_EVENTS", "ARCHIVE_ENABLE_PARSED_EVENTS", "ARCHIVE_ENABLE_SIGNALS", "SQLITE_ENABLE", "SQLITE_DB_PATH", "OPPORTUNITY_ENABLE"); print(json.dumps({key: getattr(c, key, None) for key in keys}, ensure_ascii=False, indent=2, sort_keys=True))'

compile:
	$(PY) -m compileall $(APP) $(REPORTS)

run:
	$(PY) $(APP)/main.py

run-research:
	DEFAULT_USER_TIER=research MARKET_CONTEXT_ADAPTER_MODE=live MARKET_CONTEXT_PRIMARY_VENUE=okx_perp MARKET_CONTEXT_SECONDARY_VENUE=kraken_futures $(PY) $(APP)/main.py

health:
	$(PY) -m $(APP).quality_reports --market-context-health

coverage:
	$(PY) -m $(APP).quality_reports --major-pool-coverage

quality-summary:
	$(PY) -m $(APP).quality_reports --summary

quality-csv:
	mkdir -p $(REPORTS) && $(PY) -m $(APP).quality_reports --format csv > $(REPORTS)/quality_rows.csv

db-init:
	$(PY) -m $(APP).sqlite_store --init

db-summary:
	$(PY) -m $(APP).sqlite_store --summary

db-integrity:
	$(PY) -m $(APP).sqlite_store --integrity-check

db-report:
	$(PY) -m $(APP).quality_reports --db-summary
	$(PY) -m $(APP).quality_reports --db-integrity
	$(PY) -m $(APP).quality_reports --opportunity-db-summary

db-size:
	$(PY) -m $(APP).quality_reports --db-size-breakdown

db-value-audit:
	$(PY) -m $(APP).quality_reports --db-value-audit

db-retention:
	$(PY) -m $(APP).quality_reports --db-retention-recommendation

db-compact-dry-run:
	$(PY) -m $(APP).sqlite_store --compact --dry-run

db-compact-execute:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to compact. Use make db-compact-execute CONFIRM=YES"; exit 2; fi
	$(PY) -m $(APP).sqlite_store --compact --execute

db-vacuum:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to vacuum. Use make db-vacuum CONFIRM=YES"; exit 2; fi
	CONFIRM=YES $(PY) -m $(APP).sqlite_store --vacuum

db-maintenance:
	$(MAKE) db-size
	$(MAKE) db-value-audit
	$(MAKE) db-retention
	$(MAKE) db-compact-dry-run

db-migrate-all:
	$(PY) -m $(APP).sqlite_store --migrate-archive --all

db-migrate-date:
	@if [ -z "$(DATE)" ]; then echo "Usage: make db-migrate-date DATE=YYYY-MM-DD"; exit 2; fi
	$(PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"

sqlite-checkpoint:
	$(PY) -m $(APP).sqlite_store --checkpoint

archive-status:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-status DATE=YYYY-MM-DD"; exit 2; fi
	$(PY) -m $(APP).archive_maintenance --status-date "$(DATE)"

archive-compress-dry-run:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-compress-dry-run DATE=YYYY-MM-DD"; exit 2; fi
	$(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run

archive-compress-date:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES"; exit 2; fi
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to compress. Use make archive-compress-date DATE=$(DATE) CONFIRM=YES"; exit 2; fi
	ALLOW_TODAY="$(ALLOW_TODAY)" $(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute

db-prune-dry-run:
	$(PY) -m $(APP).sqlite_store --prune --dry-run

db-prune-execute:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to prune. Use make db-prune-execute CONFIRM=YES"; exit 2; fi
	$(PY) -m $(APP).sqlite_store --prune --execute

report-overnight:
	$(PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py

report-state:
	$(PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py

report-run:
	$(PY) $(REPORTS)/generate_overnight_run_analysis_latest.py

report-all:
	$(MAKE) report-overnight
	$(MAKE) report-state
	$(MAKE) report-run

daily-close:
	@if [ -z "$(DATE)" ]; then echo "Usage: make daily-close DATE=YYYY-MM-DD [COMPRESS=YES]"; exit 2; fi
	$(PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"
	$(PY) -m $(APP).sqlite_store --integrity-check
	$(PY) -m $(APP).quality_reports --db-summary
	$(PY) -m $(APP).quality_reports --db-integrity
	$(PY) -m $(APP).quality_reports --opportunity-db-summary
	$(PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)"
	$(PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py
	$(PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py
	$(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run
	@if [ "$(COMPRESS)" = "YES" ]; then ALLOW_TODAY="$(ALLOW_TODAY)" $(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute; else echo "Compression dry-run only. Use make daily-close DATE=$(DATE) COMPRESS=YES to gzip archive."; fi
	$(PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)"
	$(PY) -m $(APP).sqlite_store --checkpoint

daily-close-strict:
	@if [ -z "$(DATE)" ]; then echo "Usage: make daily-close-strict DATE=YYYY-MM-DD [COMPRESS=YES]"; exit 2; fi
	$(PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"
	$(PY) -m $(APP).sqlite_store --integrity-check
	$(PY) -m $(APP).quality_reports --db-summary
	$(PY) -m $(APP).quality_reports --db-integrity --fail-on-mismatch
	$(PY) -m $(APP).quality_reports --opportunity-db-summary
	$(PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)" --strict
	$(PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py
	$(PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py
	$(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run
	@if [ "$(COMPRESS)" = "YES" ]; then ALLOW_TODAY="$(ALLOW_TODAY)" $(PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute; else echo "Compression dry-run only. Use make daily-close-strict DATE=$(DATE) COMPRESS=YES to gzip archive."; fi
	$(PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)" --strict
	$(PY) -m $(APP).sqlite_store --checkpoint

smoke:
	$(MAKE) compile
	$(MAKE) db-integrity
	$(MAKE) health
	$(MAKE) coverage
	$(MAKE) db-report

test-sqlite:
	$(PY) -m unittest \
	  $(APP).test_sqlite_schema \
	  $(APP).test_sqlite_writers \
	  $(APP).test_sqlite_archive_mirror \
	  $(APP).test_sqlite_opportunity_persistence \
	  $(APP).test_sqlite_reports \
	  $(APP).test_sqlite_migration

test-opportunity:
	$(PY) -m unittest \
	  $(APP).test_trade_opportunity_scoring \
	  $(APP).test_trade_opportunity_funnel \
	  $(APP).test_trade_opportunity_notifier \
	  $(APP).test_trade_opportunity_budget \
	  $(APP).test_trade_opportunity_reports \
	  $(APP).test_trade_opportunity_persistence

test-state:
	$(PY) -m unittest \
	  $(APP).test_asset_market_state \
	  $(APP).test_no_trade_lock \
	  $(APP).test_prealert_lifecycle \
	  $(APP).test_chase_candidate_validation \
	  $(APP).test_outcome_market_price_source \
	  $(APP).test_telegram_suppression \
	  $(APP).test_trade_state_reports

test-action:
	$(PY) -m unittest \
	  $(APP).test_trade_action_inference \
	  $(APP).test_trade_action_conflict \
	  $(APP).test_trade_action_notifier \
	  $(APP).test_trade_action_reports

test-lp:
	$(PY) -m unittest \
	  $(APP).test_lp_major_prealert \
	  $(APP).test_major_pool_coverage \
	  $(APP).test_lp_latency_outcomes \
	  $(APP).test_quality_report_rates \
	  $(APP).test_raw_parsed_archive

test-core:
	$(PY) -m unittest \
	  $(APP).test_operational_intent_notifier \
	  $(APP).test_clmm_operational_intent

test-all:
	$(MAKE) test-sqlite
	$(MAKE) test-opportunity
	$(MAKE) test-state
	$(MAKE) test-action
	$(MAKE) test-lp
	$(MAKE) test-core

archive-check:
	ls -lh $(APP)/data/archive/raw_events || true
	ls -lh $(APP)/data/archive/parsed_events || true
	ls -lh $(APP)/data/archive/signals || true
	ls -lh $(APP)/data/archive/delivery_audit || true
	ls -lh $(APP)/data/archive/cases || true
	ls -lh $(APP)/data/archive/case_followups || true

git-info:
	git status --short
	git log -1 --oneline

preflight:
	$(MAKE) env-check
	$(MAKE) smoke
	$(MAKE) archive-check
	$(MAKE) git-info
