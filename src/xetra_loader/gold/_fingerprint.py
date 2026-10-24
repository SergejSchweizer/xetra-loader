"""Bounded parallel construction of deterministic Gold JSON fingerprints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from math import ceil
from typing import TypeVar

import __main__

RecordT = TypeVar("RecordT")
RowSerializer = Callable[[RecordT], Mapping[str, object]]
_PARALLEL_THRESHOLD = 400_000


def semantic_fingerprint(
    records: Sequence[RecordT],
    serialize_row: RowSerializer[RecordT],
    *,
    cores: int,
    prefix: bytes = b"[",
    suffix: bytes = b"]",
) -> str:
    """Hash canonical JSON while parallelizing row transformation/encoding.

    The worker count and queued chunks are bounded by ``cores``. Results are
    consumed in source order and fed incrementally to SHA-256, preserving the
    exact legacy fingerprint bytes without materializing one lake-wide JSON
    buffer. Small inputs and a one-core budget avoid process startup overhead.
    """

    if cores < 1:
        raise ValueError("Gold transformation core budget must be positive")
    digest = hashlib.sha256()
    digest.update(prefix)
    if cores == 1 or len(records) < _PARALLEL_THRESHOLD or not _can_spawn_workers():
        for index, record in enumerate(records):
            if index:
                digest.update(b",")
            digest.update(_encode_row(serialize_row(record)))
    else:
        workers = min(cores, len(records))
        chunk_size = max(512, ceil(len(records) / (workers * 4)))
        chunks = _chunks(records, chunk_size)
        jobs = ((serialize_row, chunk) for chunk in chunks)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for index, encoded_chunk in enumerate(
                executor.map(_encode_chunk, jobs, buffersize=workers)
            ):
                if index:
                    digest.update(b",")
                digest.update(encoded_chunk)
    digest.update(suffix)
    return digest.hexdigest()


def _can_spawn_workers() -> bool:
    """Avoid multiprocessing bootstrap failures in stdin/notebook callers."""

    main_path = getattr(__main__, "__file__", None)
    return isinstance(main_path, str) and not main_path.startswith("<")


def _chunks(records: Sequence[RecordT], size: int) -> Iterator[tuple[RecordT, ...]]:
    for start in range(0, len(records), size):
        yield tuple(records[start : start + size])


def _encode_chunk(
    job: tuple[RowSerializer[RecordT], tuple[RecordT, ...]],
) -> bytes:
    serializer, records = job
    return b",".join(_encode_row(serializer(record)) for record in records)


def _encode_row(row: Mapping[str, object]) -> bytes:
    return json.dumps(
        row,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
