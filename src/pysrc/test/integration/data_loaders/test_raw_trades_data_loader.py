from datetime import date
from pathlib import Path

import numpy as np
import pytest

from pysrc.data_loaders.raw_trades_data_loader import RawTradesDataLoader
from pysrc.util.types import Asset, Market

resource_path = Path(__file__).parent / "resources"


def test_initialization_error() -> None:
    with pytest.raises(ValueError) as msg:
        RawTradesDataLoader(
            resource_path=resource_path,
            asset=Asset.ADA,
            market=Market.KRAKEN_SPOT,
            since=date(year=2024, month=7, day=1),
            until=date(year=2024, month=6, day=1),
        )
    assert (
        str(msg.value)
        == "Dates since (07_01_2024) equal to or later than until (06_01_2024)"
    )

    with pytest.raises(ValueError) as msg:
        RawTradesDataLoader(
            resource_path=resource_path / "lol",
            asset=Asset.ADA,
            market=Market.KRAKEN_SPOT,
            since=date(year=2024, month=6, day=25),
            until=None,
        )
    assert "Directory for asset trades data" in str(
        msg.value
    ) and "doesn't exist" in str(msg.value)

    with pytest.raises(ValueError) as msg:
        RawTradesDataLoader(
            resource_path=resource_path,
            asset=Asset.ADA,
            market=Market.KRAKEN_SPOT,
            since=date(year=2024, month=6, day=1),
            until=None,
        )
    assert "Expected file" in str(msg.value) and "doesn't exist" in str(msg.value)


def test_get_data_error() -> None:
    loader = RawTradesDataLoader(
        resource_path=resource_path,
        asset=Asset.ADA,
        market=Market.KRAKEN_SPOT,
        since=date(year=2024, month=6, day=25),
        until=None,
    )

    with pytest.raises(ValueError) as msg:
        loader.get_data(
            since=date(year=2024, month=6, day=1),
            until=date(year=2024, month=7, day=1),
        )
    assert "Expected file" in str(msg.value) and "doesn't exist" in str(msg.value)

    with pytest.raises(ValueError) as msg:
        loader.get_data(
            since=date(year=2024, month=6, day=25),
            until=date(year=2024, month=7, day=2),
        )
    assert "Expected file" in str(msg.value) and "doesn't exist" in str(msg.value)


def test_get_data_success() -> None:
    loader = RawTradesDataLoader(
        resource_path=resource_path,
        asset=Asset.ADA,
        market=Market.KRAKEN_SPOT,
        since=date(year=2024, month=6, day=25),
        until=None,
    )

    target = np.loadtxt(
        resource_path / "trades" / "XADAZUSD" / "test.csv",
        delimiter=",",
        dtype=[("time", "u8"), ("price", "f4"), ("volume", "f4")],
    )
    trades = loader.get_data(
        since=date(year=2024, month=6, day=25),
        until=date(year=2024, month=7, day=1),
    )

    assert len(trades) == target.shape[0]
    for i in range(len(trades)):
        assert trades[i].time == target[i][0]
        assert trades[i].price == target[i][1]
        assert trades[i].quantity == target[i][2]


def test_next_success() -> None:
    loader = RawTradesDataLoader(
        resource_path=resource_path,
        asset=Asset.ADA,
        market=Market.KRAKEN_SPOT,
        since=date(year=2024, month=6, day=25),
        until=None,
    )

    target = np.loadtxt(
        resource_path / "trades" / "XADAZUSD" / "test.csv",
        delimiter=",",
        dtype=[("time", "u8"), ("price", "f4"), ("volume", "f4")],
    )

    for i in range(target.shape[0]):
        trade = loader.next()
        assert trade is not None
        assert trade.time == target[i][0]
        assert trade.price == target[i][1]
        assert trade.quantity == target[i][2]
    assert loader.next() is None
