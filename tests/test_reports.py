import pytest
import numerapi

from numerai_reports import reports
from numerai_reports import data


@pytest.fixture
def napi(monkeypatch):

    def mocked_api(*arg, **kwargs):
        entry1 = {'username': 'test1',
                  'liveAuroc': 0.500,
                  'liveLogloss': 0.6931,
                  'stake': {'value': 1, 'confidence': '0.53'},
                  'stakeResolution': {'destroyed': True},
                  'paymentStaking': {'nmrTransfer': {'value': '0.23'},
                                     'usdAmount': '1.2'}}

        return {"data": {"rounds": [
            {'status': 'RESOLVED',
             'benchmark_type': 'auroc',
             'selection': {'bCutoff': '0.501'},
             'leaderboard': [
                entry1]}
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


def test__reputation_bonus(napi):
    res = reports._reputation_bonus(100)
    # staked 1 for 2 tournamences * 50% => 1
    assert res.loc['test1']['bonus'] == 1


def test_payments(napi):
    lb = data.fetch_leaderboard(100)
    res = reports.payments(lb, "test1")
    assert res.loc["total"]['nmr_burned'] == 2
    assert res.loc["total"]['nmr_total'] == 0.46
    assert res.loc["total"]['usd_total'] == 2.4
