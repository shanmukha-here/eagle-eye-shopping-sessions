"""Exact shopping session aggregation. All persistent intermediates use SCRATCH_DIR."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

os.environ.setdefault("POLARS_MAX_THREADS", os.environ.get("NCPU", "8"))
import duckdb
from inputs import CLICK, EVENT, dictionary_table, person_map, stage_ids


def quoted(path):
    return "'" + str(path).replace("'", "''") + "'"


def log(message):
    print(message, file=sys.stderr, flush=True)


def tables(db, data, work):
    for name, frame in (
        ("people", person_map(sorted((data / "links").glob("*.csv")))),
        ("products", dictionary_table(data, "products.csv", ["product_id", "category"])),
        ("campaigns", dictionary_table(data, "campaigns.csv", ["campaign_id", "channel"])),
    ):
        path = work / f"{name}.parquet"
        frame.write_parquet(path)
        db.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM read_parquet({quoted(path)})")


def timestamp_macros(db):
    db.execute(r"""
        CREATE MACRO number(s) AS CASE WHEN regexp_full_match(s, '[0-9]+')
            THEN try_cast(coalesce(nullif(ltrim(s,'0'), ''), '0') AS BIGINT) END;
        CREATE MACRO civil(s) AS epoch_ms(try_strptime(left(s,19), '%Y-%m-%dT%H:%M:%S'));
        CREATE MACRO millis(s) AS CASE
            WHEN regexp_full_match(s,'[0-9]+') THEN number(s)
            WHEN regexp_full_match(s,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z')
                THEN civil(s)
            WHEN regexp_full_match(s,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-]([01][0-9]|2[0-3]):[0-5][0-9]')
                THEN civil(s) - (CASE substr(s,20,1) WHEN '-' THEN -1 ELSE 1 END)
                    * (try_cast(substr(s,21,2) AS INT)*60 + try_cast(right(s,2) AS INT))*60000
            END
    """)


def clean_events(source, start, end):
    return f"""
        WITH winners AS (
            SELECT DISTINCT ON (event_id) user_id, stamp, ingest, kind, product_id, price
            FROM read_parquet({source}) ORDER BY event_id, ingest
        ), parsed AS (
            SELECT *, millis(stamp) AS ts,
                CASE WHEN kind='purchase' THEN number(price) ELSE 0 END AS cents
            FROM winners
        ), valid AS (
            SELECT user_id, ts, cents,
                CASE WHEN kind='purchase' THEN product_id END AS bought
            FROM parsed
            WHERE ts >= {start} AND ts < {end} AND user_id <> '' AND product_id <> ''
              AND kind IN ('view','cart','purchase')
              AND (kind <> 'purchase' OR cents BETWEEN 1 AND 10000000)
              AND civil(ingest) <= ts + 86400000
        )
        SELECT coalesce(p.person,v.user_id) AS u, v.ts, v.cents::BIGINT AS cents,
               CASE WHEN v.bought IS NOT NULL THEN coalesce(x.category,'unknown') END AS cat
        FROM valid v LEFT JOIN people p ON v.user_id=p.alias
        LEFT JOIN products x ON v.bought=x.product_id
    """


def clean_clicks(source, start, end):
    # Copies of a click have identical values. MIN is idempotent over their
    # multiplicity. Retain the smallest click per person/time/campaign for
    # subsequent tie-breaking, retaining every distinct assisting campaign.
    return f"""
        WITH parsed AS (
            SELECT user_id, campaign_id, click_id, millis(stamp) AS ts FROM read_parquet({source})
        )
        SELECT coalesce(p.person,v.user_id) AS u, v.ts, v.campaign_id AS c, min(v.click_id) AS k
        FROM parsed v JOIN campaigns x USING (campaign_id)
        LEFT JOIN people p ON v.user_id=p.alias
        WHERE v.user_id <> '' AND v.ts >= {start}-604800000 AND v.ts < {end}
        GROUP BY ALL
    """


def clean(db, data, work, family, columns, shards, bounds, query):
    paths = sorted((data / family).glob("*.csv"))
    raw = work / (family + "-id")
    stage_ids(paths, columns, raw, shards)
    target = work / family
    target.mkdir()
    for folder in sorted(raw.iterdir()):
        dest = target / folder.name
        source = quoted(folder / "*.parquet")
        sql = query(source, *bounds)
        db.execute(f"""
            COPY (SELECT *, (hash(u) % {shards})::INTEGER AS part FROM ({sql}) v)
            TO {quoted(dest)} (FORMAT PARQUET, PARTITION_BY (part), COMPRESSION LZ4_RAW)
        """)
        shutil.rmtree(folder)
    return target


def scan_part(folder, part, empty):
    pattern = f"*/part={part}/*.parquet"
    if next(folder.glob(pattern), None) is None:
        return f"(SELECT {empty} WHERE FALSE)"
    return f"read_parquet({quoted(folder / pattern)}, hive_partitioning=false)"


def sessions(db, events):
    # RANGE makes every peer see the gap flag even if two windows visit
    # same-time rows in different orders.
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE labeled AS
        WITH gaps AS (
            SELECT *, lag(ts) OVER (PARTITION BY u ORDER BY ts) AS prev FROM {events}
        )
        SELECT u, cents, cat, max(CASE WHEN prev IS NULL OR ts-prev > 1800000 THEN ts END)
            OVER (PARTITION BY u ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
        FROM gaps;
        CREATE OR REPLACE TEMP TABLE visits AS
            SELECT u, s, epoch_ms(s)::DATE AS day, count(*) AS events, sum(cents)::BIGINT AS revenue
            FROM labeled GROUP BY u,s;
    """)


def user_daily(db, path):
    db.execute(f"""
        COPY (
            WITH amounts AS (
                SELECT u, epoch_ms(s)::DATE AS day, cat, sum(cents) AS amount
                FROM labeled WHERE cat IS NOT NULL GROUP BY ALL
            ), top_cat AS (
                SELECT u, day, first(cat ORDER BY amount DESC, cat ASC) AS top_category
                FROM amounts GROUP BY u,day
            ), days AS (
                SELECT u, day, count(*) AS sessions, sum(events)::BIGINT AS events,
                    sum(revenue)::BIGINT AS revenue_cents FROM visits GROUP BY u,day
            )
            SELECT u AS user_id, day, sessions, events, revenue_cents, top_category
            FROM days LEFT JOIN top_cat USING (u,day)
        ) TO {quoted(path)} (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)


def campaign_daily(db, clicks):
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE click_times AS
            SELECT u, ts, c, min(k) AS k FROM {clicks} GROUP BY u,ts,c;
        INSERT INTO campaign_parts
        WITH last_at_time AS (
            SELECT u, ts, arg_min(c,k) AS c FROM click_times GROUP BY u,ts
        ), assigned AS (
            SELECT v.day, v.revenue,
                CASE WHEN a.ts >= v.s-604800000 THEN a.c ELSE 'organic' END AS c
            FROM visits v ASOF LEFT JOIN last_at_time a ON v.u=a.u AND v.s>=a.ts
        )
        SELECT c, day, count(*), count(*) FILTER (WHERE revenue>0), sum(revenue)::BIGINT, 0
        FROM assigned GROUP BY c,day;
        INSERT INTO campaign_parts
        WITH prev AS (
            SELECT u,c,ts,lag(ts) OVER (PARTITION BY u,c ORDER BY ts) AS last_ts FROM click_times
        ), marked AS (
            SELECT *, sum(CASE WHEN last_ts IS NULL OR ts-last_ts>604800000 THEN 1 ELSE 0 END)
                OVER (PARTITION BY u,c ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp FROM prev
        ), spans AS (
            SELECT u,c,min(ts) AS lo,max(ts)+604800000 AS hi FROM marked GROUP BY u,c,grp
        )
        SELECT a.c,v.day,0,0,0,count(*)
        FROM visits v JOIN spans a ON v.u=a.u AND v.s BETWEEN a.lo AND a.hi
        GROUP BY a.c,v.day
    """)


def run(data, out, work):
    threads = int(os.environ.get("NCPU", "8"))
    ram = int(os.environ.get("MEM_GB", "24"))
    db = duckdb.connect(config={"threads": threads, "memory_limit": f"{max(1, int(ram * .62))}GB",
                                 "temp_directory": str(work / "spill"), "preserve_insertion_order": False})
    manifest = json.loads((data / "manifest.json").read_text())
    timestamp_macros(db)
    bounds = db.execute("SELECT civil(?), civil(?)", [manifest["window_start"], manifest["window_end"]]).fetchone()
    tables(db, data, work)
    size = sum(p.stat().st_size for p in (data / "events").glob("*.csv"))
    shard_bytes = min(384 << 20, ram * (24 << 20))
    shards = max(1, (size + shard_bytes - 1) // shard_bytes)
    log(f"identity complete; {shards} partitions")
    events = clean(db, data, work, "events", EVENT, shards, bounds, clean_events)
    log("events cleaned")
    clicks = clean(db, data, work, "clicks", CLICK, shards, bounds, clean_clicks)
    log("clicks cleaned")
    users = out / "user_daily"
    campaigns = out / "campaign_daily"
    users.mkdir(parents=True, exist_ok=True)
    campaigns.mkdir(parents=True, exist_ok=True)
    db.execute("""CREATE TEMP TABLE campaign_parts (
        c VARCHAR, day DATE, sessions BIGINT, purchases BIGINT, cents BIGINT, assists BIGINT)""")
    for part in range(shards):
        ev = scan_part(events, part, "NULL::VARCHAR AS u, NULL::BIGINT AS ts, NULL::BIGINT AS cents, NULL::VARCHAR AS cat")
        ck = scan_part(clicks, part, "NULL::VARCHAR AS u, NULL::BIGINT AS ts, NULL::VARCHAR AS c, NULL::VARCHAR AS k")
        sessions(db, ev)
        user_daily(db, users / f"part-{part}.parquet")
        campaign_daily(db, ck)
    db.execute(f"""
        COPY (
            SELECT c AS campaign_id, coalesce(x.channel,'organic') AS channel, day,
                sum(sessions)::BIGINT AS sessions, sum(purchases)::BIGINT AS purchasing_sessions,
                sum(cents)::BIGINT AS revenue_cents, sum(assists)::BIGINT AS assisted_sessions
            FROM campaign_parts p LEFT JOIN campaigns x ON p.c=x.campaign_id
            GROUP BY c,x.channel,day
        ) TO {quoted(campaigns / "part-0.parquet")} (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    db.close()


if __name__ == "__main__":
    started = time.monotonic()
    scratch = Path(os.environ.get("SCRATCH_DIR", tempfile.gettempdir()))
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as directory:
        run(Path(os.environ["DATA_DIR"]), Path(os.environ["OUTPUT_DIR"]), Path(directory))
    log(f"finished in {time.monotonic()-started:.2f}s")
