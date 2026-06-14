#!/usr/bin/env python3
"""
research_lp_removal_offline.py
==============================

Offline pre-registered study of the chain-monitor `liquidity_removal` signal.

WHAT THE SIGNAL IS
------------------
A Uniswap V2 `Burn` event on a monitored pool = both token reserves leave the
pool simultaneously => `base_net < 0 AND quote_net < 0` => `liquidity_removal`
in app/lp_analyzer.py (lines 129-133, 250-257). A UniV2 Burn is, by construction,
exactly this two-sided outflow, so Burn logs ARE the liquidity_removal population.
Definition parity with the live system is therefore exact (zero drift).

Strategy semantics under test: liquidity withdrawal => bearish => SHORT.

CORE QUESTION
-------------
Not "does the signal have predictive power" in the abstract, but: within the
realistically reachable execution window (seconds after the block), is there
still monetizable edge left after front-runners and costs?

================================================================================
PRE-REGISTERED DECISION CRITERIA  (LOCKED — DO NOT EDIT AFTER SEEING RESULTS)
================================================================================
Entry offsets tested:   tau in {0, 30, 60, 120, 300} seconds after block time t0
Forward windows:        k   in {5, 15, 30, 60} minutes after entry
Direction primary:      SHORT (bearish prior). LONG reported symmetrically too.
Price source:           OKX 1m perp (ETH-USDT-SWAP / BTC-USDT-SWAP), close for
                        entry, low/high for excursions. Entry price = close of the
                        1m bar covering floor(t0+tau). (Nearest prior bar if gap.)
Costs (pre-registered):
    fee      = 0.10%  (perp taker 0.05% x 2 legs) = 10.0 bps round trip
    slippage = 1 tick per side (also reported at 3 ticks). Ticks are ~0 bps at
               ETH/BTC price levels; fee dominates.
Decision gates:
    P1 PREDICTIVE : event-group SHORT downside significantly exceeds the
                    unconditional same-symbol random-entry baseline (Welch t > 2).
    P2 REACHABLE  : at tau in {30,60}s, captured SHORT amplitude still
                    significantly positive (edge not fully eaten pre/at block).
    P3 NET>0      : at tau in {30,60}s, net edge (gross - 10bps - 1 tick) has
                    mean > 0 AND mean & median same sign.
    P4 MECHANISM  : size-stratified edge is monotone (larger outflow -> stronger
                    follow-through) OR direction is stable across strata.
    P5 ROBUST     : time-split (first half vs second half) reproduces the
                    conclusion. If sample too small to split -> DOWNGRADE, stated.

Verdict: all pass -> PROJECT CANDIDATE. Any fail -> named cause:
    a) no predictive power            (P1 fails)
    b) real but front-run / eaten     (P1 ok, P2 fails) -> not executable
    c) reachable but thinner than cost (P2 ok, P3 fails)
    d) too sparse to validate         (P5 undeterminable / N too low)

"Free data / old project / operator saw it work" are NOT reasons to relax any
gate. If the data looks bad, it gets written down as bad.
================================================================================

USAGE
    python3 scripts/research_lp_removal_offline.py backfill   # pull Burn logs + ts
    python3 scripts/research_lp_removal_offline.py analyze    # stats + outputs
"""
import json, os, sys, time, math, sqlite3, urllib.request, urllib.error, datetime, random, statistics, glob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data", "backfill_12m")
OUT_DIR = os.path.join(ROOT, "reports", "lp_removal_offline_study_20260614")
PRICE_DB = "/home/yiast/vnpy_projects/cta_strategy/.vntrader/database_mainnet.db"  # READ-ONLY
RPC_URL = "https://eth.drpc.org"   # public, no-key (we do NOT read .env / use Alchemy key)
BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"  # UniV2 Burn(address,uint,uint,address)
CHUNK = 10000           # drpc max getLogs block range
DB_TZ_OFFSET_SEC = 8 * 3600   # verified: cta_strategy DB datetime is UTC+8

# Pre-registered constants
TAUS = [0, 30, 60, 120, 300]
KS_MIN = [5, 15, 30, 60]
FEE_BPS = 10.0
# Study population floor on TOTAL removed liquidity (= 2x stable leg).
# Primary = config LP_PREALERT_MIN_USD (the live system's LP delivery gate). Dust
# burns (median ~$3) are excluded as non-informative automated micro-removals.
MIN_SIZE_USD_PRIMARY = 1500.0       # app/config.py LP_PREALERT_MIN_USD default
SIZE_FLOORS_REPORTED = [1500.0, 10000.0, 50000.0, 100000.0]
TICK = {"ETH": 0.01, "BTC": 0.1}     # OKX perp tick sizes
PRICE_SYMBOL = {"ETH": "ETHUSDT_SWAP_OKX", "BTC": "BTCUSDT_SWAP_OKX"}

DECIMALS = {  # token contract -> decimals
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,   # WBTC
}
STABLES = {"0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}


def log(msg):
    line = f"[{datetime.datetime.utcnow().isoformat()}Z] {msg}"
    print(line, flush=True)
    with open(os.path.join(OUT_DIR, "run_log.txt"), "a") as f:
        f.write(line + "\n")


def rpc(method, params, tries=14, timeout=20):
    """drpc free tier returns HTTP 408 'Request timeout' ~37% of the time; these are
    transient (not IP rate-limit) and a quick retry usually succeeds. Param/range
    errors (400/-32602) are raised immediately."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(RPC_URL, data=body,
                                         headers={"Content-Type": "application/json", "User-Agent": "lpstudy/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                d = json.load(r)
            if "error" in d:
                last = d["error"]
                if isinstance(last, dict) and last.get("code") in (-32602, -32600):
                    raise RuntimeError(f"rpc param error: {last}")
                time.sleep(min(0.3 * (i + 1), 2.0)); continue
            return d["result"]
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code == 400:
                raise
            time.sleep(min(0.3 * (i + 1), 2.0))   # 408 etc: fast retry
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)[:80]}"
            time.sleep(min(0.5 * (i + 1), 2.5))
    raise RuntimeError(f"rpc failed after {tries}: {method} last={last}")


def get_logs_resilient(addr, fb, tb):
    """Try the full range; if it keeps failing, split into 2k sub-ranges so a chunk
    never blocks the whole backfill."""
    try:
        return rpc("eth_getLogs", [{"fromBlock": hex(fb), "toBlock": hex(tb),
                                    "address": addr, "topics": [BURN_TOPIC]}])
    except Exception as e:
        log(f"  chunk {fb}-{tb} full-range failed ({str(e)[:50]}); splitting into 2k")
        out = []
        for sfb in range(fb, tb + 1, 2000):
            stb = min(sfb + 1999, tb)
            out += rpc("eth_getLogs", [{"fromBlock": hex(sfb), "toBlock": hex(stb),
                                        "address": addr, "topics": [BURN_TOPIC]}])
        return out


def load_pools():
    pools = json.load(open(os.path.join(ROOT, "data", "lp_pools.json")))
    out = []
    for p in pools:
        t0 = p["token0_contract"].lower()
        t1 = p["token1_contract"].lower()
        underlying = "ETH" if ("WETH".lower() in (p["token0_symbol"].lower(), p["token1_symbol"].lower())
                               or "eth" in p["pair_label"].lower()) else "BTC"
        out.append({
            "pair": p["pair_label"], "addr": p["pool_address"].lower(),
            "t0": t0, "t1": t1, "t0sym": p["token0_symbol"], "t1sym": p["token1_symbol"],
            "underlying": underlying,
        })
    return out


def decode_burn(logrow, pool):
    """UniV2 Burn data = amount0 (32 bytes) + amount1 (32 bytes)."""
    data = logrow["data"][2:]
    amt0 = int(data[0:64], 16)
    amt1 = int(data[64:128], 16)
    d0 = DECIMALS.get(pool["t0"], 18)
    d1 = DECIMALS.get(pool["t1"], 18)
    a0 = amt0 / (10 ** d0)
    a1 = amt1 / (10 ** d1)
    # stable leg gives USD; total pulled liquidity ~ 2x stable leg
    if pool["t0"] in STABLES:
        stable_usd = a0
    elif pool["t1"] in STABLES:
        stable_usd = a1
    else:
        stable_usd = None
    return {"amount0": a0, "amount1": a1, "stable_usd": stable_usd,
            "size_usd": (2 * stable_usd) if stable_usd is not None else None}


def backfill():
    os.makedirs(DATA_DIR, exist_ok=True)
    pools = load_pools()
    cur = int(rpc("eth_blockNumber", []), 16)
    # 12-month window ending ~ price-data end (DB ends 2026-05-28). Backfill generously; filter by ts later.
    end_block = cur - 110_000          # ~ -15 days from now (within price coverage)
    start_block = end_block - 2_628_000  # ~ 12 months (365d * 7200)
    log(f"backfill: cur={cur} window blocks [{start_block},{end_block}] pools={len(pools)} chunk={CHUNK}")
    ck_path = os.path.join(DATA_DIR, "backfill_checkpoint.json")
    burns_path = os.path.join(DATA_DIR, "burns_raw.jsonl")
    done = set()
    if os.path.exists(ck_path):
        done = set(json.load(open(ck_path)).get("done", []))
        log(f"resume: {len(done)} chunks already done")
    import threading
    from concurrent.futures import ThreadPoolExecutor
    fout = open(burns_path, "a")
    chunks = list(range(start_block, end_block, CHUNK))
    tasks = [(pool, fb) for pool in pools for fb in chunks
             if f"{pool['addr']}:{fb}" not in done]
    total_tasks = len(chunks) * len(pools)
    lock = threading.Lock()
    state = {"new": 0, "completed": 0}
    log(f"threaded backfill: {len(tasks)} tasks remaining of {total_tasks} (8 workers)")

    def work(task):
        pool, fb = task
        tb = min(fb + CHUNK - 1, end_block)
        try:
            res = get_logs_resilient(pool["addr"], fb, tb)
        except Exception as e:
            with lock:
                log(f"  CHUNK FAILED {pool['pair']} {fb}-{tb}: {str(e)[:60]} (will retry on resume)")
            return
        recs = []
        for lr in res:
            dec = decode_burn(lr, pool)
            recs.append({"pair": pool["pair"], "pool": pool["addr"], "underlying": pool["underlying"],
                         "block": int(lr["blockNumber"], 16), "tx": lr["transactionHash"], **dec})
        with lock:
            for rec in recs:
                fout.write(json.dumps(rec) + "\n")
            state["new"] += len(recs)
            state["completed"] += 1
            done.add(f"{pool['addr']}:{fb}")
            if state["completed"] % 20 == 0:
                fout.flush()
                json.dump({"done": sorted(done), "total_chunks": total_tasks}, open(ck_path, "w"))
                log(f"  done={len(done)}/{total_tasks} burns_new={state['new']}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(work, tasks))
    fout.flush(); fout.close()
    json.dump({"done": sorted(done), "total_chunks": total_tasks}, open(ck_path, "w"))
    log(f"backfill logs done: burn rows written this run={state['new']}")
    fetch_timestamps()


def load_burns_dedup(min_size=0.0):
    """Load burns, dedup (crash-resume can duplicate rows), keep size>=min_size."""
    seen = set(); out = []
    for line in open(os.path.join(DATA_DIR, "burns_raw.jsonl")):
        if not line.strip():
            continue
        r = json.loads(line)
        key = (r["pool"], r["block"], r.get("tx"), round(r.get("size_usd") or 0, 2))
        if key in seen:
            continue
        seen.add(key)
        if r.get("size_usd") is not None and r["size_usd"] >= min_size:
            out.append(r)
    return out


def fetch_timestamps():
    ts_path = os.path.join(DATA_DIR, "block_ts.json")
    # Only fetch timestamps for economically meaningful removals (>= primary floor);
    # dust burns are not part of the study population, so we don't pay to time them.
    rows = load_burns_dedup(min_size=MIN_SIZE_USD_PRIMARY)
    blocks = sorted({r["block"] for r in rows})
    ts = {}
    if os.path.exists(ts_path):
        ts = {int(k): v for k, v in json.load(open(ts_path)).items()}
    todo = sorted(b for b in blocks if b not in ts)
    log(f"timestamps: {len(blocks)} unique burn-blocks, {len(todo)} to fetch (single-call, threaded)")
    # drpc throttles BATCH requests but serves SINGLE eth_getBlockByNumber reliably at any depth.
    import threading
    from concurrent.futures import ThreadPoolExecutor
    lock = threading.Lock(); state = {"done": 0, "fail": 0}

    def one(b):
        try:
            res = rpc("eth_getBlockByNumber", [hex(b), False])
            t = int(res["timestamp"], 16)
            with lock:
                ts[b] = t; state["done"] += 1
                if state["done"] % 200 == 0:
                    json.dump({str(k): v for k, v in ts.items()}, open(ts_path, "w"))
                    log(f"  ts {state['done']}/{len(todo)} (fail={state['fail']})")
        except Exception as e:
            with lock:
                state["fail"] += 1
                if state["fail"] <= 5:
                    log(f"  ts fail block {b}: {str(e)[:50]}")

    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(one, todo))
    json.dump({str(k): v for k, v in ts.items()}, open(ts_path, "w"))
    log(f"timestamps done: {len(ts)} blocks resolved, {state['fail']} failed")


# ----------------------------- ANALYSIS -----------------------------

def load_prices():
    """Return {sym: {minute_utc_unix: (open,high,low,close)}} from cta_strategy DB (READ-ONLY, datetime is UTC+8)."""
    conn = sqlite3.connect(f"file:{PRICE_DB}?mode=ro", uri=True)
    out = {}
    for u, sym in PRICE_SYMBOL.items():
        m = {}
        for dt, o, h, l, c in conn.execute(
                "SELECT datetime,open_price,high_price,low_price,close_price FROM dbbardata "
                "WHERE symbol=? AND interval='1m'", (sym,)):
            # dt is UTC+8 string -> convert to UTC unix minute
            t = datetime.datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")
            utc_unix = int(t.replace(tzinfo=datetime.timezone.utc).timestamp()) - DB_TZ_OFFSET_SEC
            m[utc_unix - (utc_unix % 60)] = (o, h, l, c)
        out[u] = m
    conn.close()
    return out


def bar_at(prices_u, minute):
    b = prices_u.get(minute)
    if b: return b
    for back in range(1, 6):
        b = prices_u.get(minute - 60 * back)
        if b: return b
    return None


def short_metrics(prices_u, entry_unix, ks_min):
    """SHORT from entry. Returns per-k: max favorable downside bps & close move bps. (LONG = negation.)"""
    em = entry_unix - (entry_unix % 60)
    eb = bar_at(prices_u, em)
    if not eb: return None
    p_entry = eb[3]  # close of entry bar
    res = {}
    for k in ks_min:
        lows = []; closes = []
        for mm in range(em + 60, em + 60 * k + 1, 60):
            bb = prices_u.get(mm)
            if bb:
                lows.append(bb[2]); closes.append(bb[3])
        if not closes:
            res[k] = None; continue
        mfe_down_bps = (p_entry - min(lows)) / p_entry * 1e4    # >0 good for short
        close_bps = (p_entry - closes[-1]) / p_entry * 1e4      # >0 good for short
        res[k] = {"mfe_down_bps": mfe_down_bps, "close_bps": close_bps}
    return {"p_entry": p_entry, "k": res}


def mean_med(xs):
    xs = [x for x in xs if x is not None]
    if not xs: return {"n": 0, "mean": None, "median": None}
    return {"n": len(xs), "mean": statistics.mean(xs),
            "median": statistics.median(xs),
            "std": statistics.pstdev(xs) if len(xs) > 1 else 0.0}


def welch_t(a, b):
    a = [x for x in a if x is not None]; b = [x for x in b if x is not None]
    if len(a) < 2 or len(b) < 2: return None
    ma, mb = statistics.mean(a), statistics.mean(b)
    va, vb = statistics.variance(a), statistics.variance(b)
    se = math.sqrt(va / len(a) + vb / len(b))
    if se == 0: return None
    return (ma - mb) / se


def analyze():
    burns_path = os.path.join(DATA_DIR, "burns_raw.jsonl")
    ts = {int(k): v for k, v in json.load(open(os.path.join(DATA_DIR, "block_ts.json"))).items()}
    prices = load_prices()
    # price coverage in UTC
    cov = {u: (min(prices[u]), max(prices[u])) for u in prices}
    log(f"price coverage UTC: " + ", ".join(f"{u}:[{datetime.datetime.utcfromtimestamp(a)}..{datetime.datetime.utcfromtimestamp(b)}]" for u, (a, b) in cov.items()))

    all_rows = load_burns_dedup(min_size=MIN_SIZE_USD_PRIMARY)
    events = []
    for r in all_rows:
        b = r["block"]
        if b not in ts: continue
        r["t0"] = ts[b]
        u = r["underlying"]
        lo, hi = cov[u]
        # require pre-window (5m) and forward window (65m) fully covered
        if r["t0"] < lo + 300 or r["t0"] > hi - 3900: continue
        events.append(r)
    events.sort(key=lambda x: x["t0"])
    # population counts at each reported floor (for honesty about sample sizing)
    floor_counts = {str(int(f)): sum(1 for e in events if e["size_usd"] >= f) for f in SIZE_FLOORS_REPORTED}
    log(f"events usable (>= ${MIN_SIZE_USD_PRIMARY:.0f}, price-covered): {len(events)}; by floor {floor_counts}")

    win_start = min(e["t0"] for e in events) if events else None
    win_end = max(e["t0"] for e in events) if events else None

    # ---- decay curves: per tau, SHORT mfe_down & close, mean/median over k ----
    decay = {"SHORT": {}, "LONG": {}}
    per_event = []
    for e in events:
        u = e["underlying"]; pu = prices[u]
        row = {"t0": e["t0"], "underlying": u, "size_usd": e["size_usd"], "pair": e["pair"], "tau": {}}
        for tau in TAUS:
            sm = short_metrics(pu, e["t0"] + tau, KS_MIN)
            row["tau"][tau] = sm
        per_event.append(row)
    for direction in ("SHORT", "LONG"):
        sign = 1 if direction == "SHORT" else -1
        for tau in TAUS:
            decay[direction][tau] = {}
            for k in KS_MIN:
                mfe = [sign * pe["tau"][tau]["k"][k]["mfe_down_bps"] for pe in per_event
                       if pe["tau"][tau] and pe["tau"][tau]["k"].get(k)]
                clo = [sign * pe["tau"][tau]["k"][k]["close_bps"] for pe in per_event
                       if pe["tau"][tau] and pe["tau"][tau]["k"].get(k)]
                # NOTE: mfe is favorable-excursion; under LONG we recompute properly below
                decay[direction][tau][k] = {"close": mean_med(clo)}
        # proper MFE for LONG needs upside excursion; recompute close only is symmetric.
    # Proper directional MFE (upside for long) — recompute from bars:
    decay = directional_decay(events, prices)

    # ---- baseline: random entries, same symbols, same window ----
    baseline = random_baseline(prices, win_start, win_end, n_per_symbol=6000)

    # ---- frontrun: pre-event returns ----
    frontrun = frontrun_diag(events, prices)

    # ---- size strata ----
    strata = size_strata(events, prices)

    # ---- time split ----
    tsplit = time_split(events, prices, win_start, win_end)

    # ---- target consistency ----
    bytarget = {u: directional_decay([e for e in events if e["underlying"] == u], prices)
                for u in ("ETH", "BTC")}

    # ---- cost accounting & gates ----
    gates, verdict = evaluate_gates(events, prices, decay, baseline, strata, tsplit)

    # decay by size floor (does edge strengthen for larger removals? P4 mechanism)
    by_floor = {}
    for f in SIZE_FLOORS_REPORTED:
        sub = [e for e in events if e["size_usd"] >= f]
        d = directional_decay(sub, prices) if len(sub) >= 5 else None
        by_floor[str(int(f))] = {
            "n": len(sub),
            "short_mfe_tau60_k30": (d["SHORT"][60][30]["mfe_bps"] if d else None),
            "short_close_tau60_k30": (d["SHORT"][60][30]["close_bps"] if d else None),
            "short_close_tau0_k30": (d["SHORT"][0][30]["close_bps"] if d else None),
        }

    out = {
        "window_utc": [iso(win_start), iso(win_end)] if win_start else None,
        "primary_size_floor_usd": MIN_SIZE_USD_PRIMARY,
        "floor_counts": floor_counts,
        "decay_by_size_floor": by_floor,
        "n_events": len(events),
        "n_events_by_target": {u: sum(1 for e in events if e["underlying"] == u) for u in ("ETH", "BTC")},
        "price_coverage_utc": {u: [iso(a), iso(b)] for u, (a, b) in cov.items()},
        "decay_curve": decay,
        "baseline": baseline,
        "frontrun": frontrun,
        "size_strata": strata,
        "time_split": tsplit,
        "by_target": bytarget,
        "gates": gates,
        "verdict": verdict,
        "preregistered": {"taus_sec": TAUS, "k_min": KS_MIN, "fee_bps": FEE_BPS,
                          "tick": TICK, "direction_primary": "SHORT"},
    }
    json.dump(out, open(os.path.join(OUT_DIR, "results.json"), "w"), indent=2, default=float)
    json.dump([{k: v for k, v in e.items() if k != "tx"} for e in events],
              open(os.path.join(OUT_DIR, "event_sample.json"), "w"), indent=2, default=float)
    log("analyze done -> results.json, event_sample.json")
    return out


def directional_decay(events, prices):
    """For each tau, SHORT and LONG favorable MFE + close, mean/median over k."""
    out = {"SHORT": {}, "LONG": {}}
    for tau in TAUS:
        for direction in ("SHORT", "LONG"):
            out[direction][tau] = {}
        for k in KS_MIN:
            short_mfe, short_clo, long_mfe, long_clo = [], [], [], []
            for e in events:
                pu = prices[e["underlying"]]
                entry = e["t0"] + tau
                em = entry - (entry % 60)
                eb = bar_at(pu, em)
                if not eb: continue
                p = eb[3]
                lows, highs, closes = [], [], []
                for mm in range(em + 60, em + 60 * k + 1, 60):
                    bb = pu.get(mm)
                    if bb: lows.append(bb[2]); highs.append(bb[1]); closes.append(bb[3])
                if not closes: continue
                short_mfe.append((p - min(lows)) / p * 1e4)
                short_clo.append((p - closes[-1]) / p * 1e4)
                long_mfe.append((max(highs) - p) / p * 1e4)
                long_clo.append((closes[-1] - p) / p * 1e4)
            out["SHORT"][tau][k] = {"mfe_bps": mean_med(short_mfe), "close_bps": mean_med(short_clo)}
            out["LONG"][tau][k] = {"mfe_bps": mean_med(long_mfe), "close_bps": mean_med(long_clo)}
    return out


def random_baseline(prices, win_start, win_end, n_per_symbol=6000):
    random.seed(42)
    out = {}
    for u, pu in prices.items():
        minutes = [m for m in pu if win_start and win_start <= m <= win_end]
        if len(minutes) < 100:
            out[u] = {"n": 0}; continue
        samp = random.sample(minutes, min(n_per_symbol, len(minutes)))
        per_k = {k: {"short_mfe": [], "short_close": []} for k in KS_MIN}
        for m in samp:
            eb = pu.get(m)
            if not eb: continue
            p = eb[3]
            for k in KS_MIN:
                lows, closes = [], []
                for mm in range(m + 60, m + 60 * k + 1, 60):
                    bb = pu.get(mm)
                    if bb: lows.append(bb[2]); closes.append(bb[3])
                if not closes: continue
                per_k[k]["short_mfe"].append((p - min(lows)) / p * 1e4)
                per_k[k]["short_close"].append((p - closes[-1]) / p * 1e4)
        out[u] = {str(k): {"mfe_bps": mean_med(v["short_mfe"]), "close_bps": mean_med(v["short_close"])}
                  for k, v in per_k.items()}
    return out


def frontrun_diag(events, prices):
    """Pre-event signed returns into t0 (negative => price already falling = front-run/residual)."""
    res = {"pre5m_close_bps": [], "pre1m_close_bps": []}
    for e in events:
        pu = prices[e["underlying"]]
        m0 = (e["t0"] - (e["t0"] % 60))
        b0 = bar_at(pu, m0)
        b5 = pu.get(m0 - 300); b1 = pu.get(m0 - 60)
        if b0 and b5: res["pre5m_close_bps"].append((b0[3] - b5[3]) / b5[3] * 1e4)
        if b0 and b1: res["pre1m_close_bps"].append((b0[3] - b1[3]) / b1[3] * 1e4)
    return {"pre5m_to_t0_bps": mean_med(res["pre5m_close_bps"]),
            "pre1m_to_t0_bps": mean_med(res["pre1m_close_bps"]),
            "note": "negative mean => price already falling before event => signal captures residual"}


def size_strata(events, prices, tau=60, k=30):
    sized = sorted([e for e in events if e["size_usd"]], key=lambda x: x["size_usd"])
    if len(sized) < 12: return {"note": "too few sized events", "n": len(sized)}
    n = len(sized); terc = [sized[:n // 3], sized[n // 3:2 * n // 3], sized[2 * n // 3:]]
    out = []
    for idx, grp in enumerate(terc):
        mfe = []
        for e in grp:
            pu = prices[e["underlying"]]; entry = e["t0"] + tau; em = entry - (entry % 60)
            eb = bar_at(pu, em)
            if not eb: continue
            p = eb[3]; lows = []
            for mm in range(em + 60, em + 60 * k + 1, 60):
                bb = pu.get(mm)
                if bb: lows.append(bb[2])
            if lows: mfe.append((p - min(lows)) / p * 1e4)
        out.append({"tercile": idx + 1, "size_usd_range": [grp[0]["size_usd"], grp[-1]["size_usd"]],
                    "short_mfe_bps_tau60_k30": mean_med(mfe)})
    means = [o["short_mfe_bps_tau60_k30"]["mean"] for o in out if o["short_mfe_bps_tau60_k30"]["mean"] is not None]
    monotone = len(means) == 3 and (means[0] <= means[1] <= means[2] or means[0] >= means[1] >= means[2])
    return {"terciles": out, "monotone": monotone}


def time_split(events, prices, win_start, win_end):
    if not events or len(events) < 40:
        return {"note": "DOWNGRADE: sample too small for reliable time-split", "n": len(events)}
    mid = (win_start + win_end) // 2
    halves = {}
    for name, sub in (("first_half", [e for e in events if e["t0"] <= mid]),
                      ("second_half", [e for e in events if e["t0"] > mid])):
        d = directional_decay(sub, prices)
        halves[name] = {"n": len(sub),
                        "short_mfe_tau60_k30": d["SHORT"][60][30]["mfe_bps"],
                        "short_close_tau60_k30": d["SHORT"][60][30]["close_bps"]}
    return halves


def evaluate_gates(events, prices, decay, baseline, strata, tsplit):
    # event-group short close bps at tau in {30,60}, pooled over targets, k=30
    def event_vals(tau, k, metric):
        vals = []
        for e in events:
            pu = prices[e["underlying"]]; entry = e["t0"] + tau; em = entry - (entry % 60)
            eb = bar_at(pu, em)
            if not eb: continue
            p = eb[3]; lows = []; closes = []
            for mm in range(em + 60, em + 60 * k + 1, 60):
                bb = pu.get(mm)
                if bb: lows.append(bb[2]); closes.append(bb[3])
            if not closes: continue
            if metric == "mfe": vals.append((p - min(lows)) / p * 1e4)
            else: vals.append((p - closes[-1]) / p * 1e4)
        return vals

    def baseline_vals(u, tau_ignored, k, metric):
        # baseline close/mfe distribution per symbol (random entries); approximate by recomputing sample
        return None

    # P1: event short close vs baseline close (use close at k=30, tau=0 entry = block time)
    ev_close_30 = event_vals(0, 30, "close")
    # baseline pooled across symbols by weighting event symbol mix
    base_close = []
    for u in ("ETH", "BTC"):
        bk = baseline.get(u, {}).get("30")
        # baseline stored as summary; rebuild raw not kept -> approximate using mean/median only
    # For a real t-test we recompute baseline raw here:
    base_raw = recompute_baseline_raw(prices, events, tau=0, k=30, metric="close")
    t_p1 = welch_t(ev_close_30, base_raw)
    P1 = (t_p1 is not None and t_p1 > 2.0)

    # P2: tau in {30,60} mfe significantly positive (one-sample t vs 0)
    def one_sample_t(xs):
        xs = [x for x in xs if x is not None]
        if len(xs) < 2: return None
        m = statistics.mean(xs); s = statistics.pstdev(xs)
        if s == 0: return None
        return m / (s / math.sqrt(len(xs)))
    p2_ts = {}
    for tau in (30, 60):
        v = event_vals(tau, 30, "mfe")
        p2_ts[tau] = {"mean": mean_med(v)["mean"], "t_vs0": one_sample_t(v)}
    P2 = all(p2_ts[tau]["t_vs0"] is not None and p2_ts[tau]["t_vs0"] > 2.0 for tau in (30, 60))

    # P3: net edge = captured mfe - cost, tau in {30,60}; need realistic captured = close move (not MFE peek-ahead)
    # Use close move at k=30 as realizable (cannot time the exact low). Net = close_bps - FEE - slippage.
    P3_detail = {}
    for tau in (30, 60):
        clo = event_vals(tau, 30, "close")
        mm = mean_med(clo)
        # slippage 1 tick on a ~mid price (use median underlying price proxy ~ negligible)
        slip_bps = 0.04  # ~1 tick at ETH/BTC scale, reported; ~0
        net_mean = (mm["mean"] - FEE_BPS - slip_bps) if mm["mean"] is not None else None
        net_med = (mm["median"] - FEE_BPS - slip_bps) if mm["median"] is not None else None
        P3_detail[tau] = {"gross_close_bps": mm, "net_mean_bps": net_mean, "net_median_bps": net_med}
    P3 = all(P3_detail[tau]["net_mean_bps"] is not None and P3_detail[tau]["net_mean_bps"] > 0
             and (P3_detail[tau]["net_median_bps"] or -1) > 0 for tau in (30, 60))

    # P4
    P4 = bool(strata.get("monotone"))
    # P5
    P5 = "DOWNGRADE" not in json.dumps(tsplit)

    gates = {
        "P1_predictive": {"pass": bool(P1), "welch_t_event_vs_baseline_close_t0_k30": t_p1,
                          "event_mean_bps": mean_med(ev_close_30)["mean"],
                          "baseline_mean_bps": mean_med(base_raw)["mean"], "n_event": len(ev_close_30)},
        "P2_reachable": {"pass": bool(P2), "detail": p2_ts},
        "P3_net_positive": {"pass": bool(P3), "detail": P3_detail, "fee_bps": FEE_BPS},
        "P4_mechanism": {"pass": bool(P4), "monotone": strata.get("monotone")},
        "P5_robust": {"pass": bool(P5), "time_split": tsplit},
    }
    # verdict
    if not P1: cause = "a) no predictive power (P1)"
    elif not P2: cause = "b) real but front-run/eaten (P2) - not executable"
    elif not P3: cause = "c) reachable but thinner than cost (P3)"
    elif not P5: cause = "d) too sparse to validate (P5)"
    else: cause = None
    allpass = all([P1, P2, P3, P4, P5])
    verdict = {"all_pass": allpass, "fail_cause": cause,
               "decision": "PROJECT CANDIDATE" if allpass else f"NO PROJECT - {cause}"}
    return gates, verdict


def recompute_baseline_raw(prices, events, tau, k, metric):
    random.seed(7)
    # match symbol mix of events
    out = []
    win = [e["t0"] for e in events]
    lo, hi = min(win), max(win)
    mix = {u: sum(1 for e in events if e["underlying"] == u) for u in ("ETH", "BTC")}
    for u, cnt in mix.items():
        pu = prices[u]
        minutes = [m for m in pu if lo <= m <= hi]
        if not minutes: continue
        target = max(cnt * 30, 1000)
        samp = random.sample(minutes, min(target, len(minutes)))
        for m in samp:
            entry = m + tau; em = entry - (entry % 60)
            eb = bar_at(pu, em)
            if not eb: continue
            p = eb[3]; lows = []; closes = []
            for mm in range(em + 60, em + 60 * k + 1, 60):
                bb = pu.get(mm)
                if bb: lows.append(bb[2]); closes.append(bb[3])
            if not closes: continue
            out.append((p - (min(lows) if metric == "mfe" else closes[-1])) / p * 1e4)
    return out


def iso(unix):
    return datetime.datetime.utcfromtimestamp(unix).strftime("%Y-%m-%d %H:%M:%SZ") if unix else None


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "analyze"
    if cmd == "backfill":
        backfill()
    elif cmd == "timestamps":
        fetch_timestamps()
    elif cmd == "analyze":
        analyze()
    else:
        print("usage: research_lp_removal_offline.py [backfill|timestamps|analyze]")
