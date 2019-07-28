import pytest

from numerai_reports import reports

from . import napi


def test_all_star_club(napi):
    res = reports.all_star_club(100)
    assert len(res) == 1


def test_pass_rate(napi):
    res = reports.pass_rate(100)
    assert res.loc['mean']['all'] == 0.5


def test_reputation_bonus(napi):
    res = reports.reputation_bonus(100)
    # staked 1 for 1 tournamenc * 50% => 0.5
    assert res.loc['loser']['bonus'] == 0.5
    assert res.loc['winner']['bonus'] == 1.5


def test_friends(napi):
    res = reports.friends('loser', 100, 101)
    assert len(res) == 1


def test_payments(napi):
    res = reports.payments("loser", 100)
    assert res.loc["total"]['nmr_burned'] == 1
    assert res.loc["total"]['nmr_total'] == -1
    assert res.loc["total"]['usd_total'] == 0
    res = reports.payments("winner", 100)
    assert res.loc["total"]['nmr_burned'] == 0
    assert res.loc["total"]['nmr_total'] == 0.76
    assert res.loc["total"]['usd_total'] == 3.4
    # later round with reputation bonus
    # combined report
    res = reports.payments(['winner', 'loser'], 160)
    assert res.loc["total"]['nmr_burned'] == 1
    assert res.loc["total"]['nmr_rep_bonus'] == 2
    assert res.loc["total"]['nmr_total'] == 1.43
    assert res.loc["total"]['usd_total'] == 1.2


def test_out_of_n(napi):
    res = reports.out_of_n(100, 101)
    assert res.loc['mean'].tolist() == [0.5, 0.5, 2, 1.0]


def test_dominance(napi):
    res = reports.dominance("winner", 100)
    assert res.loc[100]['bernie'] == 1
    res = reports.dominance("loser", 100)
    assert res.loc[100]['bernie'] == 0


def test_reputation(napi):
    res = reports.reputation(['loser', 'winner'], 140, 150)

    assert res.loc['121-140'].round(3).tolist() == [0.491, 0.514]
    assert res.loc['131-150'].round(3).tolist() == [0.500, 0.512]


def test_summary(napi):
    res = reports.summary(100, 120)
    assert res.loc[111].round(3).tolist() == [2.0, 4.0, 2.0, 0.5, 0.5]
