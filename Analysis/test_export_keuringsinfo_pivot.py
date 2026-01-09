import datetime as dt

from Analysis.export_keuringsinfo import (
    KeuringsRecord,
    _build_pivot,
    _pivot_result_key,
)


def test_pivot_result_key_cutoff_and_blank_handling():
    cutoff = dt.date(2021, 1, 1)

    # date == cutoff => doesn't count => 'geen keuring'
    r1 = KeuringsRecord(
        toezichtgroep="V&W-WL",
        type="LS",
        match="single_ls",
        uuid="1",
        naam=None,
        naampad=None,
        isActief=True,
        toestand="in-gebruik",
        datum_laatste_keuring="2021-01-01",
        resultaat_keuring="conform",
    )
    assert _pivot_result_key(r1, cutoff=cutoff) == "geen keuring"

    # date > cutoff but blank result => 'geen keuring'
    r2 = r1.__class__(
        toezichtgroep="V&W-WL",
        type="LS",
        match="single_ls",
        uuid="2",
        naam=None,
        naampad=None,
        isActief=True,
        toestand="in-gebruik",
        datum_laatste_keuring="2021-01-02",
        resultaat_keuring="  ",
    )
    assert _pivot_result_key(r2, cutoff=cutoff) == "geen keuring"

    # date > cutoff and non-blank result => counted as itself
    r3 = r1.__class__(
        toezichtgroep="V&W-WL",
        type="LS",
        match="single_ls",
        uuid="3",
        naam=None,
        naampad=None,
        isActief=True,
        toestand="in-gebruik",
        datum_laatste_keuring="2022-02-03",
        resultaat_keuring="conform",
    )
    assert _pivot_result_key(r3, cutoff=cutoff) == "conform"


def test_build_pivot_counts_every_record_by_default_and_excludes_not_meegenomen():
    cutoff = dt.date(2021, 1, 1)

    records = [
        KeuringsRecord(
            toezichtgroep="V&W-WL",
            type="LS",
            match="single_ls",
            uuid="1",
            naam=None,
            naampad=None,
            isActief=True,
            toestand="in-gebruik",
            datum_laatste_keuring="2022-01-01",
            resultaat_keuring="conform",
        ),
        KeuringsRecord(
            toezichtgroep="V&W-WL",
            type="LS",
            match="single_ls",
            uuid="2",
            naam=None,
            naampad=None,
            isActief=True,
            toestand="in-gebruik",
            datum_laatste_keuring=None,
            resultaat_keuring=None,
        ),
        KeuringsRecord(
            toezichtgroep="V&W-WL",
            type="LS",
            match="single_ls",
            uuid="3",
            naam=None,
            naampad=None,
            isActief=True,
            toestand="verwijderd",  # not meegenomen
            datum_laatste_keuring="2022-01-01",
            resultaat_keuring="conform",
        ),
    ]

    cols, counters = _build_pivot(records, cutoff=cutoff, include_not_meegenomen=False)
    assert set(cols) == {"conform", "geen keuring"}
    assert counters["V&W-WL"]["conform"] == 1
    assert counters["V&W-WL"]["geen keuring"] == 1

    cols2, counters2 = _build_pivot(records, cutoff=cutoff, include_not_meegenomen=True)
    assert set(cols2) == {"conform", "geen keuring"}
    assert counters2["V&W-WL"]["conform"] == 2
    assert counters2["V&W-WL"]["geen keuring"] == 1
