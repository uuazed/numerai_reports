import os
import pytest
import numerapi


# turn off fetching from cloud bucket
os.environ["NO_CLOUD_BuCKET"] = "True"


@pytest.fixture
def napi(monkeypatch):

    def mocked_api(napi, query, args=None, **kwargs):
        if "v2Leaderboard" in query:
            items = [
              {
                "corrRep": 0.0781832522274644,
                "fncRank": 1298,
                "fncRep": 0.00520445944779911,
                "mmcRank": 3,
                "mmcRep": 0.0337998327994166,
                "rank": 1,
                "username": "one"
              },
              {
                "corrRep": 0.0751630937016408,
                "fncRank": 99,
                "fncRep": 0.0133189179721062,
                "mmcRank": 17,
                "mmcRep": 0.0275337967611373,
                "rank": 2,
                "username": "two"
              },
              {
                "corrRep": 0.0724161004058988,
                "fncRank": 1714,
                "fncRep": -0.000021793387548435,
                "mmcRank": 13,
                "mmcRep": 0.0286524851715735,
                "rank": 3,
                "username": "three"
              },
              {
                "corrRep": 0.071796609297472,
                "fncRank": 903,
                "fncRep": 0.00850229929269823,
                "mmcRank": 43,
                "mmcRep": 0.0217568682811675,
                "rank": 4,
                "username": "four"
              },
              {
                "corrRep": 0.0716320107658114,
                "fncRank": 625,
                "fncRep": 0.0100559924674553,
                "mmcRank": 15,
                "mmcRep": 0.0279660865762948,
                "rank": 5,
                "username": "five"
              },
              ]
            return {"data": {"v2Leaderboard": items}}

        elif "v2UserProfile" in query:
            if args['username'] in ['one', 'two', 'three', 'four', 'five']:
                base = {
                      "correlation": 0.023985807161517148,
                      "correlationWithMetamodel": 0.5794202300023875,
                      "date": "2021-02-24T00:00:00Z",
                      "fnc": -0.005286782583341209,
                      "leaderboardBonus": "0.000000000000000000",
                      "mmc": 0.00846393867443868,
                      "payoutPending": "0.086358895526745387",
                      "roundNumber": 248,
                      "selectedStakeValue": "2.661311924084651658"
                    }

                return {
                  "data": {
                    "v2UserProfile": {
                      "accountId": "0145d36b-8204-4d16-8bc7-80611826b830",
                      "id": "af64b78c-7cb0-4813-9357-fcf98a6ca042",
                      "latestRoundPerformances": [base],
                      "medals": {
                        'gold': 1,
                        'silver': 2,
                        'bronze': 3
                      }
                      }
                    }
                  }
            else:
                return {"data": None}
        else:
            raise "unpatched api call"

    monkeypatch.setattr(numerapi.numerapi.NumerAPI, "raw_query", mocked_api)
