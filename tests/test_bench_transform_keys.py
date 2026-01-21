import time
from typing import Any, Dict

import pytest

from InitialFillStep import InitialFillStep


def _make_payload(width: int = 50, depth: int = 2) -> Dict[str, Any]:
    """Create a synthetic EMSON-like payload with namespaces and dotted keys.

    No real data; purely for timing.
    """
    leaf: Dict[str, Any] = {
        "tz:DtcToezichter.id": "00000000-0000-0000-0000-000000000000",
        "tz:DtcToezichter.naam": "X",
        "tz:DtcToezichter.gebruikersnaam": "u",
    }

    obj: Dict[str, Any] = {
        "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000000-FAKEASSET",
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
    }

    for i in range(width):
        obj[f"ns{i}:A.b.c"] = f"v{i}"

    # nested
    cur: Dict[str, Any] = obj
    for d in range(depth):
        cur[f"tz:Toezicht.toezichter{d}"] = dict(leaf)
        cur[f"bs:Bestek.bestekkoppeling{d}"] = [
            {
                "bs:DtcBestekkoppeling.bestekId": {"DtcIdentificator.identificator": "00000000-0000-0000-0000-000000000000-XXX"},
                "bs:DtcBestekkoppeling.status": "https://example.invalid/status/actief",
            }
        ]
        nxt: Dict[str, Any] = {}
        cur[f"x:inner{d}"] = nxt
        cur = nxt

    return obj


@pytest.mark.benchmark
def test_bench_transform_keys_smoke():
    payload = _make_payload(width=80, depth=3)

    # Warmup
    InitialFillStep._transform_keys(payload)

    n = 200
    t0 = time.perf_counter()
    for _ in range(n):
        InitialFillStep._transform_keys(payload)
    dt = time.perf_counter() - t0

    # Not asserting on timing: just print so you can compare before/after locally.
    print(f"_transform_keys: {n} iterations took {dt:.4f}s -> {n/dt:.1f} iter/s")
