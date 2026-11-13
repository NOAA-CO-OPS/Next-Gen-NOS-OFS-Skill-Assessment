"""Offline USGS retrieve/inventory tests (mocked searvey — runs on every PR)."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from ofs_skill.obs_retrieval import inventory_usgs_station, retrieve_usgs_station
from tests.helpers.api_mocks import (
    make_usgs_searvey_raw,
    mock_usgs_searvey,
)


@pytest.fixture
def logger():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('test_usgs_mocked')


@pytest.mark.integration
def test_retrieve_water_temperature_mocked(logger):
    """USGS temperature retrieve path with searvey patched out."""
    raw = make_usgs_searvey_raw(code='00010', value=15.0, periods=12)
    with mock_usgs_searvey(timeseries=raw):
        result = retrieve_usgs_station(
            SimpleNamespace(
                station='01646500',
                start_date='20240101',
                end_date='20240102',
                variable='water_temperature',
            ),
            logger,
        )

    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {'DateTime', 'DEP01', 'OBS'}
    assert len(result) == 12
    assert result['OBS'].iloc[0] == pytest.approx(15.0)


@pytest.mark.integration
def test_retrieve_water_level_feet_to_meters_mocked(logger):
    """Code 00065 (feet) is converted to meters."""
    raw = make_usgs_searvey_raw(code='00065', value=3.28084, periods=6)
    with mock_usgs_searvey(timeseries=raw):
        result = retrieve_usgs_station(
            SimpleNamespace(
                station='01646500',
                start_date='20240101',
                end_date='20240102',
                variable='water_level',
            ),
            logger,
        )

    assert result is not None
    assert 'Datum' in result.columns
    # 3.28084 ft ≈ 1.0 m
    assert result['OBS'].iloc[0] == pytest.approx(1.0, abs=1e-4)


@pytest.mark.integration
def test_retrieve_empty_returns_none(logger):
    with mock_usgs_searvey(timeseries=pd.DataFrame()):
        result = retrieve_usgs_station(
            SimpleNamespace(
                station='01646500',
                start_date='20240101',
                end_date='20240102',
                variable='water_level',
            ),
            logger,
        )
    assert result is None


@pytest.mark.integration
def test_inventory_usgs_station_mocked(logger, monkeypatch):
    """Inventory discovery without live Water Data API or API key."""
    monkeypatch.delenv('API_USGS_PAT', raising=False)
    with mock_usgs_searvey():
        inv = inventory_usgs_station(
            [38.0, 40.0, -78.0, -76.0],
            '20240101',
            '20240102',
            logger,
        )

    assert not inv.empty
    assert inv.iloc[0]['ID'] == '01646500'
    assert inv.iloc[0]['Source'] == 'USGS'
    assert bool(inv.iloc[0]['has_wl']) is True
