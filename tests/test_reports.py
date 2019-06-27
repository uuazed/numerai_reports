import pytest
import numerapi

from numerai_reports import reports
from numerai_reports import data


@pytest.fixture
def napi(monkeypatch):

    def mocked_api(*arg, **kwargs):
        entry1 = {'username': 'looser',
                  'liveAuroc': 0.500,
                  'liveLogloss': 0.6931,
                  'stake': {'value': 1, 'confidence': '0.53'},
                  'stakeResolution': {'destroyed': True},
                  'paymentStaking': None}
        entry2 = {'username': 'winner',
                  'liveAuroc': 0.512,
                  'liveLogloss': 0.6921,
                  'stake': {'value': 3, 'confidence': '0.52'},
                  'stakeResolution': {'destroyed': False},
                  'paymentStaking': {'nmrTransfer': {'value': '0.23'},
                                     'usdAmount': '1.2'}}
        return {"data": {"rounds": [
            {'status': 'RESOLVED',
             'benchmark_type': 'auroc',
             'selection': {'bCutoff': '0.501'},
             'leaderboard': [
                entry1, entry2]}
            ]}}

    def mocked_tournaments(*args, **kwargs):
        return [{'active': True,
                 'name': 'bernie',
                 'tournament': 1},
                {'active': False,
                 'name': 'ken',
                 'tournament': 4}
                ]

    monkeypatch.setattr(numerapi.numerapi.NumerAPI, "raw_query", mocked_api)
    monkeypatch.setattr(
        numerapi.numerapi.NumerAPI, "get_tournaments",
        mocked_tournaments)


def test_reputation_bonus(napi):
    res = reports.reputation_bonus(100)
    # staked 1 for 2 tournamences * 50% => 1
    assert res.loc['looser']['bonus'] == 1
    assert res.loc['winner']['bonus'] == 3


def test_payments(napi):
    lb = data.fetch_leaderboard(100)
    res = reports.payments(lb, "looser")
    assert res.loc["total"]['nmr_burned'] == 2
    assert res.loc["total"]['nmr_total'] == -2
    assert res.loc["total"]['usd_total'] == 0
    res = reports.payments(lb, "winner")
    assert res.loc["total"]['nmr_burned'] == 0
    assert res.loc["total"]['nmr_total'] == 0.46
    assert res.loc["total"]['usd_total'] == 2.4


def test_dominance(napi):
    lb = data.fetch_leaderboard(100)
    res = reports.dominance(lb, "winner")
    assert res.loc[100]['bernie'] == 1
    res = reports.dominance(lb, "looser")
    assert res.loc[100]['bernie'] == 0
