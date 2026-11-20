"""Frame by physical LF, then apply the track's field grammar to complete rows."""
from pathlib import Path

import polars as pl


EVENT = ["event_id", "user_id", "stamp", "ingest", "kind", "product_id", "price"]
CLICK = ["click_id", "user_id", "campaign_id", "stamp"]
LINK = ["a", "b"]
BATCH_BYTES = 12 << 20


def read_batches(files, columns):
    field = '(?:[^,"\\n]*|"[^,"\\n]*")'
    pattern = "^" + ",".join([field] * len(columns)) + r"\z"
    pending = []
    size = 0

    def decode():
        series = pl.Series("line", pending, dtype=pl.Binary).cast(pl.String)
        valid = series.filter(series.str.contains(pattern))
        fields = valid.str.replace_all('"', "", literal=True).str.split_exact(",", len(columns) - 1)
        return fields.struct.rename_fields(columns).struct.unnest()

    for path in files:
        with path.open("rb") as source:
            source.readline()  # Only the first physical row is a header.
            while block := source.read(BATCH_BYTES - size):
                block += source.readline()  # finish the last physical record
                lines = block.split(b"\n")
                if lines[-1] == b"":
                    lines.pop()
                pending.extend(lines)
                size += len(block)
                if size >= BATCH_BYTES:
                    yield decode()
                    pending.clear()
                    size = 0
    if pending:
        yield decode()


def dictionary_table(data, name, columns):
    batches = list(read_batches([data / name], columns))
    return pl.concat(batches) if batches else pl.DataFrame(schema=dict.fromkeys(columns, pl.String))


def person_map(paths):
    # Attach the byte-wise larger root to the smaller. Path halving keeps
    # long alias chains cheap. UTF-8 lexical order agrees with scalar order.
    parents = {}

    def root(node):
        while node != parents[node]:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    for batch in read_batches(paths, LINK):
        for a, b in batch.iter_rows():
            if not a or not b:
                continue
            parents.setdefault(a, a)
            parents.setdefault(b, b)
            a, b = root(a), root(b)
            if a != b:
                parents[max(a, b)] = min(a, b)
    return pl.DataFrame(
        {"alias": list(parents), "person": [root(key) for key in parents]},
        schema={"alias": pl.String, "person": pl.String},
    )


def stage_ids(paths, columns, folder: Path, shards):
    """All copies of an ID must land together, even across files and people."""
    folder.mkdir()
    identifier = columns[0]
    for serial, frame in enumerate(read_batches(paths, columns)):
        frame = frame.filter(pl.col(identifier) != "")
        if frame.is_empty():
            continue
        if shards == 1:
            parts = [((0,), frame)]
        else:
            keyed = frame.with_columns((pl.col(identifier).hash() % shards).alias("part"))
            parts = keyed.partition_by("part", as_dict=True, include_key=False).items()
        for (part,), rows in parts:
            target = folder / str(part)
            target.mkdir(exist_ok=True)
            rows.write_parquet(target / f"{serial}.parquet", compression="lz4")
