import pytest
import numerapi

from numerai_reports import reports


@pytest.fixture
def napi(monkeypatch):

    def mocked_api(napi, query, args=None, **kwargs):
        entry1 = {'username': 'loser',
                  'liveAuroc': 0.500,
                  'liveLogloss': 0.6931,
                  'stake': {'value': 1, 'confidence': '0.53'},
                  'stakeResolution': {'destroyed': True},
                  'paymentStaking': None,
                  'return': {'nmrAmount': 0}}
        entry2 = {'username': 'winner',
                  'liveAuroc': 0.512,
                  'liveLogloss': 0.6921,
                  'stake': {'value': 3, 'confidence': '0.52'},
                  'stakeResolution': {'destroyed': False},
                  'paymentStaking': {'nmrTransfer': {'value': '0.23'},
                                     'usdAmount': '1.2'}}
        entry3 = {'username': 'winner',
                  'liveAuroc': 0.520,
                  'liveLogloss': 0.6911,
                  'stake': {'value': 1.9, 'confidence': '0.12'},
                  'stakeResolution': {'destroyed': False},
                  'paymentStaking': {'nmrTransfer': {'value': '0.53'},
                                     'usdAmount': '2.2'}}
        entry4 = {'username': 'loser',
                  'liveAuroc': 0.455,
                  'liveLogloss': 0.6965,
                  'stake': None,
                  'stakeResolution': None,
                  'paymentStaking': None,
                  'return': None}
        rounds = {'data': {'tournaments':
            [{'active': True, 'tournament': 1, 'name': 'bernie',
              'rounds': [{'status': "RESOLVED", 'number': i}
                         for i in range(60, 166)]},
             {'active': True, 'tournament': 2, 'name': 'ken',
              'rounds': [{'status': "RESOLVED", 'number': i}
                         for i in range(100, 126)]}]}}

        if "leaderboard" in query:
            if args['tournament'] == 1:
                return {"data": {"rounds": [
                       {'status': 'RESOLVED',
                        'benchmark_type': 'auroc',
                        'selection': {'bCutoff': '0.501'},
                        'leaderboard': [entry1, entry2]}]}}
            elif args['tournament'] == 2:
                return {"data": {"rounds": [
                       {'status': 'RESOLVED',
                        'benchmark_type': 'auroc',
                        'selection': {'bCutoff': '0.501'},
                        'leaderboard': [entry3, entry4]}]}}
        else:
            return rounds

    monkeypatch.setattr(numerapi.numerapi.NumerAPI, "raw_query", mocked_api)


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


def test_dominance(napi):
    res = reports.dominance("winner", 100)
    assert res.loc[100]['bernie'] == 1
    res = reports.dominance("loser", 100)
    assert res.loc[100]['bernie'] == 0
