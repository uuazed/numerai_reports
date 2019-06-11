import pandas as pd
import numpy as np
import numerapi
import tqdm
from joblib import Memory

import utils


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
            paid
            successful
          }
        }
      }
    }
'''

napi = numerapi.NumerAPI(verbosity='warn')

memory = Memory("../.cache", verbose=0)


@memory.cache
def fetch_tournaments():
    return napi.get_tournaments(only_active=False)


@memory.cache
def fetch_one(round_num, tournament):
    arguments = {'number': round_num, 'tournament': tournament['tournament']}
    raw = napi.raw_query(query, arguments)['data']['rounds']
    if len(raw) > 0:
        df = pd.io.json.json_normalize(raw[0]['leaderboard'], sep='_')
        df.columns = [utils.to_snake_case(col) for col in df.columns]
        df.rename(columns={'payment_staking_nmr_amount': 'nmr_staking',
                           'payment_staking_usd_amount': 'usd_staking',
                           'return_nmr_amount': 'nmr_returned',
                           'stake_value': 'nmr_staked'},
                  inplace=True)
        df['tournament_num'] = tournament['tournament']
        df['tournament'] = tournament['name']
        df['round_num'] = round_num
        df['nmr_staked'] = df['nmr_staked'].astype(float)
        df['stake_confidence'] = df['stake_confidence'].astype(float)
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
                bonus_lost = df['nmr_staked'] * staking_bonus_perc
                bonus_won = df['nmr_staking'] - df['nmr_staking'] / (1 + staking_bonus_perc)
                df['nmr_staking_bonus'] = bonus_lost.where(df['usd_staking'].isna(), bonus_won)
                df['usd_staking_bonus'] = df['usd_staking'] - df['usd_staking'] / (1 + staking_bonus_perc)
                df['usd_staking'] = df['usd_staking'] - df['usd_staking_bonus']
                df['nmr_returned'] = df['nmr_returned'].astype(float)
                df['nmr_returned'] -= bonus_lost
                df['nmr_staking'] -= bonus_won
                df['nmr_burned'] -= df['nmr_returned']

        return df
    return None


def fetch_leaderboard(start=0, end=None):
    dfs = []
    rounds = [start] if end is None else range(start, end + 1)
    for round_num in tqdm.tqdm(rounds, desc="fetching leaderboards"):
        for tournament in fetch_tournaments():
            res = fetch_one(round_num, tournament)
            if res is not None:
                dfs.append(res)

    df = pd.concat(dfs, sort=False)

    return df
