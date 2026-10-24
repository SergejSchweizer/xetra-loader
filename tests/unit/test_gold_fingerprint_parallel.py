from __future__ import annotations

import hashlib
import json

import xetra_loader.gold._fingerprint as gold_fingerprint


def _serialize_row(row: dict[str, str]) -> dict[str, str]:
    return row


def test_parallel_gold_fingerprint_matches_canonical_legacy_bytes(monkeypatch) -> None:
    monkeypatch.setattr(gold_fingerprint, "_PARALLEL_THRESHOLD", 32_768)
    rows = [
        {"instrument": f"I{index % 97:03d}", "value": str(index), "name": "München"}
        for index in range(33_000)
    ]
    expected = hashlib.sha256(
        json.dumps(
            rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    actual = gold_fingerprint.semantic_fingerprint(rows, _serialize_row, cores=4)

    assert actual == expected


def test_gold_fingerprint_one_core_uses_the_same_canonical_encoding() -> None:
    rows = [{"value": str(index)} for index in range(20)]
    expected = hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    assert gold_fingerprint.semantic_fingerprint(rows, _serialize_row, cores=1) == expected


def test_parallel_gold_fingerprint_supports_canonical_action_envelope(monkeypatch) -> None:
    monkeypatch.setattr(gold_fingerprint, "_PARALLEL_THRESHOLD", 1)
    rows = [{"event_key": f"{index:064x}", "value": str(index)} for index in range(40)]
    retracted = [["DE0000000001", "XETRA", "ABC", "a" * 64]]
    payload = {"rows": rows, "retracted_keys": retracted}
    expected = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    prefix = (
        b'{"retracted_keys":'
        + json.dumps(retracted, separators=(",", ":")).encode()
        + b',"rows":['
    )

    actual = gold_fingerprint.semantic_fingerprint(
        rows,
        _serialize_row,
        cores=4,
        prefix=prefix,
        suffix=b"]}",
    )

    assert actual == expected
