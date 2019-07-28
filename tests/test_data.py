import pytest
import pandas as pd

from numerai_reports import data

from . import napi


def test_fetch_leaderboard(napi):
    # normal behaviour
    df = data.fetch_leaderboard(100)
    assert isinstance(df, pd.DataFrame)

    # unknown round
    with pytest.raises(ValueError) as excinfo:
        data.fetch_leaderboard(-1)
    assert "no such round" in str(excinfo.value)
