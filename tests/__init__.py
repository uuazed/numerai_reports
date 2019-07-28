import os
import pytest
import numerapi

# turn off caching
os.environ["NOCACHE"] = "True"

# turn off rate limits
os.environ["NORATELIMIT"] = "True"


@pytest.fixture
def napi(monkeypatch):

    def mocked_api(napi, query, args=None, **kwargs):
        entry1 = {'username': 'loser',
                  'liveAuroc': 0.500,
                  'liveLogloss': 0.6931,
                  'usdGeneral': 0.0,
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
