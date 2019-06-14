import os

import pandas as pd
import numpy as np
import numerapi
import tqdm
from joblib import Memory

from numerai_reports import utils


query = '''
    query($number: Int!
          $tournament: Int!) {
      rounds(number: $number
             tournament: $tournament) {
        benchmark_type
        status
        selection {
          bCutoff
          pCutoff
        }
        leaderboard {
          liveAuroc
          liveLogloss
          username
          validationAuroc
          paymentStaking {
            nmrTransfer {
              value
            }
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
            paid
            successful
          }
        }
      }
    }
'''

napi = numerapi.NumerAPI(verbosity='warn')

# allow turning off caching, for example during unit tests
nocache_flag = os.environ.get('NOCACHE', False)
memory = Memory(None if nocache_flag else "../.cache", verbose=0)


@memory.cache
def api_fetch_tournaments():
    return napi.get_tournaments(only_active=False)


@memory.cache
def api_fetch_leaderboard(round_num, tournament):
    arguments = {'number': round_num, 'tournament': tournament['tournament']}
    return napi.raw_query(query, arguments)['data']['rounds']


@memory.cache
def fetch_one(round_num, tournament):
    raw = api_fetch_leaderboard(round_num, tournament)
    if len(raw) > 0:
        df = pd.io.json.json_normalize(raw[0]['leaderboard'], sep='_')
        df.columns = [utils.to_snake_case(col) for col in df.columns]
        df.rename(columns={'payment_staking_nmr_transfer_value': 'nmr_staking',
                           'payment_staking_usd_amount': 'usd_staking',
                           'return_nmr_amount': 'nmr_returned',
                           'stake_value': 'nmr_staked'},
                  inplace=True)
        df['tournament_num'] = tournament['tournament']
        df['tournament'] = tournament['name']
        df['round_num'] = round_num
        df['nmr_staked'] = df['nmr_staked'].astype(float)
        df['stake_confidence'] = df['stake_confidence'].astype(float)
        df['round_status'] = raw[0]['status']
        if raw[0]['status'] == "RESOLVED":
            df['staking_cutoff'] = raw[0]['selection']['bCutoff']
            df['benchmark_type'] = raw[0]['benchmark_type']
            if 'nmr_staking' in df:
                df['nmr_staking'] = df['nmr_staking'].astype(float)
            if 'usd_staking' in df:
                df['usd_staking'] = df['usd_staking'].astype(float)
            if raw[0]['benchmark_type'] == "auroc":
                staking_cutoff = raw[0]['selection']['bCutoff']
            else:
                staking_cutoff = raw[0]['selection']['pCutoff']
            df['staking_cutoff'] = float(staking_cutoff)

            pass_auroc = (df['live_auroc'] > df['staking_cutoff']).astype(int)
            # FIXME try to get rid of hardcoded threshold
            pass_ll = (df['live_logloss'] < 0.693).astype(int)
            df['pass'] = pass_auroc.where(df['benchmark_type'] == 'auroc', pass_ll)
            df['nmr_burned'] = (df['nmr_staked'] * df['stake_resolution_destroyed']).astype(float)

            # partial burns and reputation bonus
            # FIXME when did the staking bonus system start? 158?
            if 'nmr_returned' in df:
                staking_bonus_perc = 0.05
                bonus_nmr_only = df['nmr_staked'] * staking_bonus_perc
                bonus_split = df['nmr_staking'] - df['nmr_staking'] / (1 + staking_bonus_perc)
                df['nmr_returned'] = df['nmr_returned'].astype(float)
                if round_num == 158:
                    df['nmr_bonus'] = bonus_nmr_only.where(df['usd_staking'].isna(), bonus_nmr_only)
                    df['usd_bonus'] = df['usd_staking'] - df['usd_staking'] / (1 + staking_bonus_perc)
                    df['usd_staking'] = df['usd_staking'] - df['usd_bonus']
                    df['nmr_returned'] -= bonus_nmr_only
                    df['nmr_staking'] -= bonus_nmr_only
                if round_num > 158:
                    df['nmr_bonus'] = bonus_nmr_only
                df['nmr_burned'] -= df['nmr_returned'].fillna(0)

        return df
    return None


def fetch_leaderboard(start=0, end=None):
    dfs = []
    rounds = [start] if end is None else range(start, end + 1)
    for round_num in tqdm.tqdm(rounds, desc="fetching leaderboards"):
        for tournament in api_fetch_tournaments():
            res = fetch_one(round_num, tournament)
            if res is not None:
                dfs.append(res)

    df = pd.concat(dfs, sort=False)


    return df
