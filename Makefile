PY ?= ./venv/bin/python
APP ?= app
REPORTS ?= reports
DATE ?=
TABLE ?=
CONFIRM ?= NO
COMPRESS ?= NO
ALLOW_TODAY ?= NO
STRICT ?= NO
DB_PATH ?= data/chain_monitor.sqlite
DB_INTEGRITY_FAST ?= NO

.DEFAULT_GOAL := help

RUN_PY = SQLITE_DB_PATH="$(DB_PATH)" $(PY)
DB_INTEGRITY_FLAGS = $(if $(filter YES yes TRUE true 1,$(DB_INTEGRITY_FAST)),--fast,)
REPORT_RUN_SCRIPT = $(REPORTS)/generate_overnight_run_analysis_latest.py
ARCHIVE_DIR = $(APP)/data/archive

TEST_SQLITE_MODULES := \
	$(APP).test_sqlite_schema \
	$(APP).test_sqlite_writers \
	$(APP).test_sqlite_archive_mirror \
	$(APP).test_sqlite_opportunity_persistence \
	$(APP).test_sqlite_reports \
	$(APP).test_sqlite_migration

TEST_SQLITE_COMPACT_MODULES := \
	$(APP).test_sqlite_data_value_audit \
	$(APP).test_sqlite_compact_modes \
	$(APP).test_sqlite_compact_safety \
	$(APP).test_sqlite_retention_reports \
	$(APP).test_sqlite_migration_modes \
	$(APP).test_sqlite_payload_backfill \
	$(APP).test_sqlite_payload_backfill_safety \
	$(APP).test_sqlite_compact_after_backfill

TEST_REPORT_MODULES := \
	$(APP).test_report_db_first_loader \
	$(APP).test_report_archive_gzip_fallback \
	$(APP).test_report_db_archive_mismatch \
	$(APP).test_daily_compare_report \
	$(APP).test_daily_compare_source_selection \
	$(APP).test_daily_compare_makefile

TEST_OPPORTUNITY_MODULES := \
	$(APP).test_trade_opportunity_scoring \
	$(APP).test_trade_opportunity_funnel \
	$(APP).test_trade_opportunity_notifier \
	$(APP).test_trade_opportunity_budget \
	$(APP).test_trade_opportunity_reports \
	$(APP).test_trade_opportunity_persistence \
	$(APP).test_opportunity_exit_unified \
	$(APP).test_opportunity_notifier_gate \
	$(APP).test_opportunity_legacy_chase_downgrade \
	$(APP).test_opportunity_report_gate

TEST_STATE_MODULES := \
	$(APP).test_asset_market_state \
	$(APP).test_no_trade_lock \
	$(APP).test_prealert_lifecycle \
	$(APP).test_chase_candidate_validation \
	$(APP).test_outcome_market_price_source \
	$(APP).test_telegram_suppression \
	$(APP).test_trade_state_reports

TEST_ACTION_MODULES := \
	$(APP).test_trade_action_inference \
	$(APP).test_trade_action_conflict \
	$(APP).test_trade_action_notifier \
	$(APP).test_trade_action_reports

TEST_LP_MODULES := \
	$(APP).test_lp_major_prealert \
	$(APP).test_major_pool_coverage \
	$(APP).test_lp_latency_outcomes \
	$(APP).test_quality_report_rates \
	$(APP).test_raw_parsed_archive \
	$(APP).test_lp_sweep_phase_mapping \
	$(APP).test_lp_confirm_scope \
	$(APP).test_lp_confirm_downgrade \
	$(APP).test_lp_absorption_context

TEST_CLMM_MODULES := \
	$(APP).test_clmm_parser \
	$(APP).test_clmm_operational_intent \
	$(APP).test_clmm_replay_fixtures \
	$(APP).test_clmm_decoder_replay

TEST_CORE_MODULES := \
	$(APP).test_operational_intent_notifier \
	$(APP).test_exchange_entity_attribution \
	$(APP).test_data_fallbacks

define run_existing_tests
mods=""; \
for module in $(1); do \
	file=$$(printf '%s' "$$module" | tr '.' '/').py; \
	if [ -f "$$file" ]; then \
		mods="$$mods $$module"; \
	else \
		printf '%s\n' "skip missing test module: $$module"; \
	fi; \
done; \
if [ -z "$$(printf '%s' "$$mods" | tr -d '[:space:]')" ]; then \
	printf '%s\n' "no matching test modules found"; \
else \
	set -- $$mods; \
	printf '%s\n' "running unittest modules: $$*"; \
	SQLITE_DB_PATH="$(DB_PATH)" $(PY) -m unittest "$$@"; \
fi
endef

.PHONY: \
	help \
	env-check \
	compile \
	run \
	run-research \
	git-info \
	health \
	coverage \
	quality-summary \
	quality-csv \
	report-source \
	report-source-fast \
	opportunity-db \
	opportunity-calibration \
	db-init \
	db-summary \
	db-integrity \
	db-report \
	db-migrate-all \
	db-migrate-date \
	sqlite-checkpoint \
	db-size \
	db-value-audit \
	db-retention \
	db-maintenance \
	db-backfill-payload-dry-run \
	db-backfill-payload-execute \
	db-backfill-table-dry-run \
	db-backfill-table-execute \
	db-compact-dry-run \
	db-compact-execute \
	db-compact-table-dry-run \
	db-compact-table-execute \
	db-vacuum \
	db-prune-dry-run \
	db-prune-execute \
	archive-status \
	archive-compress-dry-run \
	archive-compress-date \
	daily-close \
	daily-close-strict \
	daily-compare \
	daily-compare-strict \
	daily-compare-rebuild \
	report-overnight \
	report-state \
	report-run \
	report-all \
	test-sqlite \
	test-sqlite-compact \
	test-reports \
	test-opportunity \
	test-state \
	test-action \
	test-lp \
	test-clmm \
	test-core \
	test-all \
	archive-check \
	smoke \
	smoke-fast \
	smoke-full \
	preflight \
	preflight-full

help:
	@printf '%s\n' "chain-monitor common targets:"
	@printf '%s\n' ""
	@printf '%s\n' "Startup / preflight:"
	@printf '%s\n' "  make help                               Show this help."
	@printf '%s\n' "  make env-check                          Print non-sensitive runtime config only."
	@printf '%s\n' "  make compile                            Compile app/ and reports/ with $(PY)."
	@printf '%s\n' "  make smoke                              Alias for smoke-fast; no archive/DB report-source scan."
	@printf '%s\n' "  make smoke-fast                         Run compile + fast DB integrity + market-context + coverage checks."
	@printf '%s\n' "  make smoke-full                         Run smoke-fast plus DB/report-source/opportunity/archive checks."
	@printf '%s\n' "  make archive-check                      List archive directories under $(ARCHIVE_DIR)."
	@printf '%s\n' "  make git-info                           Show git status and latest commit."
	@printf '%s\n' "  make preflight                          Run env-check, smoke-fast, archive-check, git-info."
	@printf '%s\n' "  make preflight-full                     Run env-check, smoke-full, archive-check, git-info."
	@printf '%s\n' ""
	@printf '%s\n' "Runtime:"
	@printf '%s\n' "  make run                                Start the main program."
	@printf '%s\n' "  make run-research                       Start with research tier and OKX/Kraken live context."
	@printf '%s\n' ""
	@printf '%s\n' "Health / quality / coverage:"
	@printf '%s\n' "  make health                             Show market context health."
	@printf '%s\n' "  make coverage                           Show ETH/BTC/SOL x USDT/USDC major pool coverage."
	@printf '%s\n' "  make quality-summary                    Show quality summary."
	@printf '%s\n' "  make quality-csv                        Export quality rows to reports/quality_rows.csv."
	@printf '%s\n' "  make report-source                      Show whether reports read SQLite first or archive fallback."
	@printf '%s\n' "  make report-source-fast                 Show report source health without scanning archive/gzip row counts."
	@printf '%s\n' "  make opportunity-db                     Show opportunity / candidate / verified / blocked DB summary."
	@printf '%s\n' "  make opportunity-calibration            Show opportunity score calibration summary."
	@printf '%s\n' ""
	@printf '%s\n' "SQLite basics:"
	@printf '%s\n' "  make db-init                            Initialize SQLite schema."
	@printf '%s\n' "  make db-summary                         Show SQLite summary."
	@printf '%s\n' "  make db-integrity                       Run SQLite integrity checks; set DB_INTEGRITY_FAST=YES for schema/count fast mode."
	@printf '%s\n' "  make db-report                          Run db-summary, db-integrity, opportunity-db-summary."
	@printf '%s\n' "  make db-migrate-all                     Mirror all archive NDJSON into SQLite."
	@printf '%s\n' "  make db-migrate-date DATE=YYYY-MM-DD    Mirror one archive date into SQLite."
	@printf '%s\n' "  make sqlite-checkpoint                  Run SQLite WAL checkpoint truncate."
	@printf '%s\n' ""
	@printf '%s\n' "SQLite size / value / compact:"
	@printf '%s\n' "  make db-size                            Show DB/WAL/table/JSON payload size breakdown."
	@printf '%s\n' "  make db-value-audit                     Show core_learning/core_outcome/core_state value audit."
	@printf '%s\n' "  make db-retention                       Show long-term retention / slim / archive-only recommendations."
	@printf '%s\n' "  make db-maintenance                     Run db-size, db-value-audit, db-retention, db-compact-dry-run."
	@printf '%s\n' "  make db-backfill-payload-dry-run        Preview legacy archive_path/payload_hash metadata backfill."
	@printf '%s\n' "  make db-backfill-payload-execute        Execute payload metadata backfill only with CONFIRM=YES."
	@printf '%s\n' "  make db-backfill-table-dry-run          Preview one-table backfill. Usage: make ... TABLE=raw_events"
	@printf '%s\n' "  make db-backfill-table-execute          Execute one-table backfill only with TABLE=... CONFIRM=YES."
	@printf '%s\n' "  make db-compact-dry-run                 Preview compact without modifying DB."
	@printf '%s\n' "  make db-compact-execute                 Execute compact only with CONFIRM=YES."
	@printf '%s\n' "  make db-compact-table-dry-run           Preview one-table compact. Usage: make ... TABLE=raw_events"
	@printf '%s\n' "  make db-compact-table-execute           Execute one-table compact only with TABLE=... CONFIRM=YES."
	@printf '%s\n' "  make db-vacuum                          Run VACUUM only with CONFIRM=YES."
	@printf '%s\n' "  make db-prune-dry-run                   Preview retention prune without deleting rows."
	@printf '%s\n' "  make db-prune-execute                   Execute retention prune only with CONFIRM=YES."
	@printf '%s\n' ""
	@printf '%s\n' "Archive / daily close:"
	@printf '%s\n' "  make archive-status DATE=YYYY-MM-DD     Show archive status for one date."
	@printf '%s\n' "  make archive-compress-dry-run DATE=YYYY-MM-DD"
	@printf '%s\n' "                                          Preview gzip archive compression for one date."
	@printf '%s\n' "  make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES [ALLOW_TODAY=YES]"
	@printf '%s\n' "                                          Execute gzip compression for one date."
	@printf '%s\n' "  make daily-close DATE=YYYY-MM-DD        Migrate, check DB, run reports, compress dry-run, checkpoint."
	@printf '%s\n' "  make daily-close DATE=YYYY-MM-DD COMPRESS=YES [CONFIRM=YES]"
	@printf '%s\n' "                                          Request gzip after dry-run; actual compression still needs CONFIRM=YES."
	@printf '%s\n' "  make daily-close-strict DATE=YYYY-MM-DD COMPRESS=YES [CONFIRM=YES]"
	@printf '%s\n' "                                          Strict mirror-check variant; actual compression still needs CONFIRM=YES."
	@printf '%s\n' "  make daily-compare [DATE=YYYY-MM-DD]    Generate today vs previous-available compare report."
	@printf '%s\n' "  make daily-compare-strict [DATE=YYYY-MM-DD]"
	@printf '%s\n' "                                          Strict compare; rebuild missing today/previous dated summaries first, then fail with strict_failure_reason if inputs stay incomplete."
	@printf '%s\n' "  make daily-compare-rebuild [DATE=YYYY-MM-DD]"
	@printf '%s\n' "                                          Rebuild today/previous dated summaries first, then generate compare in non-strict mode and emit rebuild_summary/rebuild_warnings."
	@printf '%s\n' ""
	@printf '%s\n' "Reports:"
	@printf '%s\n' "  make report-overnight                   Generate overnight trade action analysis."
	@printf '%s\n' "  make report-state                       Generate afternoon/evening state analysis."
	@printf '%s\n' "  make report-run                         Generate overnight run analysis if the script exists."
	@printf '%s\n' "  make report-all                         Generate all common reports."
	@printf '%s\n' "  make daily-compare                      宽松模式生成 today vs previous compare；缺口写 limitations。"
	@printf '%s\n' "  make daily-compare-strict               严格模式：先补 today/previous dated summaries；仍不完整就失败并写 strict_failure_reason。"
	@printf '%s\n' "  make daily-compare-rebuild              先尝试补 today/previous dated summaries，再按宽松模式输出 compare，并写 rebuild_summary。"
	@printf '%s\n' ""
	@printf '%s\n' "Tests:"
	@printf '%s\n' "  make test-sqlite                        Run SQLite schema/writer/mirror/report/migration tests."
	@printf '%s\n' "  make test-sqlite-compact                Run SQLite compact/backfill/value/retention tests."
	@printf '%s\n' "  make test-reports                       Run DB-first report loader and archive fallback tests."
	@printf '%s\n' "  make test-opportunity                   Run trade opportunity tests."
	@printf '%s\n' "  make test-state                         Run asset state / no-trade / suppression tests."
	@printf '%s\n' "  make test-action                        Run trade action inference / notifier / report tests."
	@printf '%s\n' "  make test-lp                            Run LP majors / archive / latency / coverage tests."
	@printf '%s\n' "  make test-clmm                          Run CLMM parser / replay tests."
	@printf '%s\n' "  make test-core                          Run core notifier / attribution / fallback tests."
	@printf '%s\n' "  make test-all                           Run every grouped test target."

env-check:
	@SQLITE_DB_PATH="$(DB_PATH)" $(PY) -c 'import importlib, json; c = importlib.import_module("$(APP).config"); keys = ("DEFAULT_USER_TIER", "MARKET_CONTEXT_ADAPTER_MODE", "MARKET_CONTEXT_PRIMARY_VENUE", "MARKET_CONTEXT_SECONDARY_VENUE", "ARCHIVE_ENABLE_RAW_EVENTS", "ARCHIVE_ENABLE_PARSED_EVENTS", "ARCHIVE_ENABLE_SIGNALS", "SQLITE_ENABLE", "SQLITE_DB_PATH", "SQLITE_REPORT_READ_PREFER_DB", "OPPORTUNITY_ENABLE", "ASSET_MARKET_STATE_ENABLE", "NO_TRADE_LOCK_ENABLE"); print(json.dumps({key: getattr(c, key, None) for key in keys}, ensure_ascii=False, indent=2, sort_keys=True))'

compile:
	$(PY) -m compileall $(APP) $(REPORTS)

run:
	$(RUN_PY) $(APP)/main.py

run-research:
	DEFAULT_USER_TIER=research MARKET_CONTEXT_ADAPTER_MODE=live MARKET_CONTEXT_PRIMARY_VENUE=okx_perp MARKET_CONTEXT_SECONDARY_VENUE=kraken_futures SQLITE_DB_PATH="$(DB_PATH)" $(PY) $(APP)/main.py

git-info:
	git status --short
	git log -1 --oneline

health:
	$(RUN_PY) -m $(APP).quality_reports --market-context-health

coverage:
	$(RUN_PY) -m $(APP).quality_reports --major-pool-coverage

quality-summary:
	$(RUN_PY) -m $(APP).quality_reports --summary

quality-csv:
	mkdir -p $(REPORTS)
	$(RUN_PY) -m $(APP).quality_reports --format csv > $(REPORTS)/quality_rows.csv

report-source:
	$(RUN_PY) -m $(APP).quality_reports --report-source-summary

report-source-fast:
	$(RUN_PY) -m $(APP).quality_reports --report-source-summary --fast

opportunity-db:
	$(RUN_PY) -m $(APP).quality_reports --opportunity-db-summary

opportunity-calibration:
	$(RUN_PY) -m $(APP).quality_reports --opportunity-calibration

db-init:
	$(RUN_PY) -m $(APP).sqlite_store --init

db-summary:
	$(RUN_PY) -m $(APP).sqlite_store --summary

db-integrity:
	$(RUN_PY) -m $(APP).sqlite_store --integrity-check $(DB_INTEGRITY_FLAGS)

db-report:
	$(RUN_PY) -m $(APP).quality_reports --db-summary
	$(RUN_PY) -m $(APP).quality_reports --db-integrity
	$(RUN_PY) -m $(APP).quality_reports --opportunity-db-summary

db-migrate-all:
	$(RUN_PY) -m $(APP).sqlite_store --migrate-archive --all

db-migrate-date:
	@if [ -z "$(DATE)" ]; then echo "Usage: make db-migrate-date DATE=YYYY-MM-DD"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"

sqlite-checkpoint:
	$(RUN_PY) -m $(APP).sqlite_store --checkpoint

db-size:
	$(RUN_PY) -m $(APP).quality_reports --db-size-breakdown

db-value-audit:
	$(RUN_PY) -m $(APP).quality_reports --db-value-audit

db-retention:
	$(RUN_PY) -m $(APP).quality_reports --db-retention-recommendation

db-maintenance:
	$(MAKE) db-size
	$(MAKE) db-value-audit
	$(MAKE) db-retention
	$(MAKE) db-compact-dry-run

db-backfill-payload-dry-run:
	$(RUN_PY) -m $(APP).sqlite_store --backfill-payload-metadata --dry-run

db-backfill-payload-execute:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to backfill payload metadata. Use make db-backfill-payload-execute CONFIRM=YES"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --backfill-payload-metadata --execute

db-backfill-table-dry-run:
	@if [ -z "$(TABLE)" ]; then echo "Usage: make db-backfill-table-dry-run TABLE=raw_events"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --backfill-payload-metadata --table "$(TABLE)" --dry-run

db-backfill-table-execute:
	@if [ -z "$(TABLE)" ]; then echo "Usage: make db-backfill-table-execute TABLE=raw_events CONFIRM=YES"; exit 2; fi
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to backfill one table. Use make db-backfill-table-execute TABLE=$(TABLE) CONFIRM=YES"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --backfill-payload-metadata --table "$(TABLE)" --execute

db-compact-dry-run:
	$(RUN_PY) -m $(APP).sqlite_store --compact --dry-run

db-compact-execute:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to compact. Use make db-compact-execute CONFIRM=YES"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --compact --execute

db-compact-table-dry-run:
	@if [ -z "$(TABLE)" ]; then echo "Usage: make db-compact-table-dry-run TABLE=raw_events"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --compact-table "$(TABLE)" --dry-run

db-compact-table-execute:
	@if [ -z "$(TABLE)" ]; then echo "Usage: make db-compact-table-execute TABLE=raw_events CONFIRM=YES"; exit 2; fi
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to compact one table. Use make db-compact-table-execute TABLE=$(TABLE) CONFIRM=YES"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --compact-table "$(TABLE)" --execute

db-vacuum:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to vacuum. Use make db-vacuum CONFIRM=YES"; exit 2; fi
	CONFIRM=YES SQLITE_DB_PATH="$(DB_PATH)" $(PY) -m $(APP).sqlite_store --vacuum

db-prune-dry-run:
	$(RUN_PY) -m $(APP).sqlite_store --prune --dry-run

db-prune-execute:
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to prune. Use make db-prune-execute CONFIRM=YES"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --prune --execute

archive-status:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-status DATE=YYYY-MM-DD"; exit 2; fi
	$(RUN_PY) -m $(APP).archive_maintenance --status-date "$(DATE)"

archive-compress-dry-run:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-compress-dry-run DATE=YYYY-MM-DD"; exit 2; fi
	ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run

archive-compress-date:
	@if [ -z "$(DATE)" ]; then echo "Usage: make archive-compress-date DATE=YYYY-MM-DD CONFIRM=YES"; exit 2; fi
	@if [ "$(CONFIRM)" != "YES" ]; then echo "Refusing to compress archive. Use make archive-compress-date DATE=$(DATE) CONFIRM=YES"; exit 2; fi
	ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute

daily-close:
	@if [ -z "$(DATE)" ]; then echo "Usage: make daily-close DATE=YYYY-MM-DD [COMPRESS=YES] [CONFIRM=YES]"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"
	$(RUN_PY) -m $(APP).sqlite_store --integrity-check
	$(RUN_PY) -m $(APP).quality_reports --db-summary
	$(RUN_PY) -m $(APP).quality_reports --db-integrity
	$(RUN_PY) -m $(APP).quality_reports --opportunity-db-summary
	$(RUN_PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)"
	$(RUN_PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py
	$(RUN_PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py
	@if [ -f "$(REPORT_RUN_SCRIPT)" ]; then $(RUN_PY) $(REPORT_RUN_SCRIPT); else echo "report-run script not found"; fi
	ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run
	@if [ "$(COMPRESS)" = "YES" ]; then \
		if [ "$(CONFIRM)" != "YES" ]; then \
			echo "Compression requested but skipped without CONFIRM=YES. Re-run make daily-close DATE=$(DATE) COMPRESS=YES CONFIRM=YES"; \
		else \
			ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute; \
		fi; \
	else \
		echo "Compression dry-run only. Use make daily-close DATE=$(DATE) COMPRESS=YES CONFIRM=YES to gzip archive."; \
	fi
	$(RUN_PY) -m $(APP).sqlite_store --checkpoint

daily-close-strict:
	@if [ -z "$(DATE)" ]; then echo "Usage: make daily-close-strict DATE=YYYY-MM-DD [COMPRESS=YES] [CONFIRM=YES]"; exit 2; fi
	$(RUN_PY) -m $(APP).sqlite_store --migrate-archive --date "$(DATE)"
	$(RUN_PY) -m $(APP).sqlite_store --integrity-check
	$(RUN_PY) -m $(APP).quality_reports --db-summary
	$(RUN_PY) -m $(APP).quality_reports --db-integrity
	$(RUN_PY) -m $(APP).quality_reports --opportunity-db-summary
	$(RUN_PY) -m $(APP).archive_maintenance --mirror-check-date "$(DATE)" --strict
	$(RUN_PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py
	$(RUN_PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py
	@if [ -f "$(REPORT_RUN_SCRIPT)" ]; then $(RUN_PY) $(REPORT_RUN_SCRIPT); else echo "report-run script not found"; fi
	ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --dry-run
	@if [ "$(COMPRESS)" = "YES" ]; then \
		if [ "$(CONFIRM)" != "YES" ]; then \
			echo "Compression requested but skipped without CONFIRM=YES. Re-run make daily-close-strict DATE=$(DATE) COMPRESS=YES CONFIRM=YES"; \
		else \
			ALLOW_TODAY="$(ALLOW_TODAY)" $(RUN_PY) -m $(APP).archive_maintenance --compress-date "$(DATE)" --execute; \
		fi; \
	else \
		echo "Compression dry-run only. Use make daily-close-strict DATE=$(DATE) COMPRESS=YES CONFIRM=YES to gzip archive."; \
	fi
	$(RUN_PY) -m $(APP).sqlite_store --checkpoint

daily-compare:
	@mkdir -p $(REPORTS)/daily_compare
	@if [ -n "$(DATE)" ]; then \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py --date "$(DATE)"; \
	else \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py; \
	fi

daily-compare-strict:
	@mkdir -p $(REPORTS)/daily_compare
	@if [ -n "$(DATE)" ]; then \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py --strict --date "$(DATE)"; \
	else \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py --strict; \
	fi

daily-compare-rebuild:
	@mkdir -p $(REPORTS)/daily_compare
	@if [ -n "$(DATE)" ]; then \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py --rebuild --date "$(DATE)"; \
	else \
		$(RUN_PY) $(REPORTS)/generate_daily_compare_report.py --rebuild; \
	fi

report-overnight:
	$(RUN_PY) $(REPORTS)/generate_overnight_trade_action_analysis_latest.py

report-state:
	$(RUN_PY) $(REPORTS)/generate_afternoon_evening_state_analysis_latest.py

report-run:
	@if [ -f "$(REPORT_RUN_SCRIPT)" ]; then $(RUN_PY) $(REPORT_RUN_SCRIPT); else echo "report-run script not found"; fi

report-all:
	$(MAKE) report-overnight
	$(MAKE) report-state
	$(MAKE) report-run

test-sqlite:
	@$(call run_existing_tests,$(TEST_SQLITE_MODULES))

test-sqlite-compact:
	@$(call run_existing_tests,$(TEST_SQLITE_COMPACT_MODULES))

test-reports:
	@$(call run_existing_tests,$(TEST_REPORT_MODULES))

test-opportunity:
	@$(call run_existing_tests,$(TEST_OPPORTUNITY_MODULES))

test-state:
	@$(call run_existing_tests,$(TEST_STATE_MODULES))

test-action:
	@$(call run_existing_tests,$(TEST_ACTION_MODULES))

test-lp:
	@$(call run_existing_tests,$(TEST_LP_MODULES))

test-clmm:
	@$(call run_existing_tests,$(TEST_CLMM_MODULES))

test-core:
	@$(call run_existing_tests,$(TEST_CORE_MODULES))

test-all:
	$(MAKE) test-sqlite
	$(MAKE) test-sqlite-compact
	$(MAKE) test-reports
	$(MAKE) test-opportunity
	$(MAKE) test-state
	$(MAKE) test-action
	$(MAKE) test-lp
	$(MAKE) test-clmm
	$(MAKE) test-core

archive-check:
	@ls -ld $(ARCHIVE_DIR)/raw_events || true
	@ls -ld $(ARCHIVE_DIR)/parsed_events || true
	@ls -ld $(ARCHIVE_DIR)/signals || true
	@ls -ld $(ARCHIVE_DIR)/delivery_audit || true
	@ls -ld $(ARCHIVE_DIR)/cases || true
	@ls -ld $(ARCHIVE_DIR)/case_followups || true

smoke: smoke-fast

smoke-fast:
	$(MAKE) compile
	$(MAKE) db-integrity DB_INTEGRITY_FAST=YES
	$(MAKE) health
	$(MAKE) coverage

smoke-full:
	$(MAKE) smoke-fast
	$(MAKE) db-report
	$(MAKE) report-source
	$(MAKE) opportunity-db
	$(MAKE) archive-check

preflight:
	$(MAKE) env-check
	$(MAKE) smoke-fast
	$(MAKE) archive-check
	$(MAKE) git-info

preflight-full:
	$(MAKE) env-check
	$(MAKE) smoke-full
	$(MAKE) archive-check
	$(MAKE) git-info
