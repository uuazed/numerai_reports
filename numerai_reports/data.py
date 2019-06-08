import decimal

import pandas as pd
import numerapi
import tqdm
from joblib import Memory

import utils


query = '''
    query($number: Int!
          $tournament: Int!) {
      rounds(number: $number
             tournament: $tournament) {
        leaderboard {
          liveAuroc
          liveLogloss
          username
          validationAuroc
          paymentStaking {
            nmrAmount
            usdAmount
          }
          stake {
            value
            confidence
          }
          return {
            nmrAmount
          }
          stakeResolution {
            destroyed
          }
        }
      }
    }
'''

napi = numerapi.NumerAPI(verbosity='warn')

memory = Memory("../.cache", verbose=0)


def _parse_float_string(s):
    if isinstance(s, str):
        return numerapi.utils.parse_float_string(s)
    return decimal.Decimal(0)


@memory.cache
def fetch_one(round_num, tournament):
    arguments = {'number': round_num, 'tournament': tournament['tournament']}
    raw = napi.raw_query(query, arguments)['data']['rounds']
    if len(raw) > 0:
        leaderboard = [utils.flatten(d) for d in raw[0]['leaderboard']]
        df = pd.DataFrame(leaderboard)
        df['tournament_num'] = tournament['tournament']
        df['tournament'] = tournament['name']
        df['round_num'] = round_num
        df['staking_cutoff'] = str(napi.get_staking_cutoff(
            round_num, tournament['tournament']))
        return df
    return None


def fetch_leaderboard(start=0, end=None):
    dfs = []
    rounds = [start] if end is None else range(start, end + 1)
    for round_num in tqdm.tqdm(rounds):
        for tournament in napi.get_tournaments(only_active=False):
            res = fetch_one(round_num, tournament)
            if res is not None:
                dfs.append(res)

    df = pd.concat(dfs, sort=False)
    df.columns = [utils.to_snake_case(col) for col in df.columns]

    df.rename(columns={'payment_staking_nmr_amount': 'nmr_staking',
                       'payment_staking_usd_amount': 'usd',
                       'return_nmr_amount': 'nmr_returned',
                       'stake_value': 'nmr_staked'},
              inplace=True)
    df['nmr_staking'] = df['nmr_staking'].apply(_parse_float_string)
    df['nmr_staked'] = df['nmr_staked'].apply(_parse_float_string)
    df['usd'] = df['usd'].apply(_parse_float_string)
    df['pass'] = (df['live_auroc'] > 0.501).astype(int)
    df['nmr_burned'] = df['nmr_staked'] * df['stake_resolution_destroyed']
    if 'nmr_returned' in df:
        df['nmr_returned'] = df['nmr_returned'].apply(_parse_float_string)
        df['nmr_burned'] -= df['nmr_returned']

    # 5% staking bonus introduced in round 158 included in 'usd' and 'nmr_staking'
    return df
