import pytest

from . import napi


def test_all_star_club(napi):
    from numerai_reports import reports
    res = reports.all_star_club(100)
    assert len(res) == 1
