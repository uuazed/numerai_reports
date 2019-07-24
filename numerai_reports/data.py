import os
import logging

import pandas as pd
import numpy as np
import numerapi
import tqdm
from joblib import Memory
from ratelimit import limits, sleep_and_retry

import utils


logger = logging.getLogger(__name__)


query_old = '''
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
          paymentGeneral {
            nmrTransfer {
              value
            }
            usdAmount
            nmrAmount
          }
          stake {
            value
            confidence
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
          liveCorrelation
          username
          validationAuroc
          validationCorrelation
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

# allow turning off rate limiting, for example during unit tests
noratelimit_flag = os.environ.get('NORATELIMIT', False)
period = 0 if noratelimit_flag else 30
# allow turning off caching, for example during unit tests
nocache_flag = os.environ.get('NOCACHE', False)
memory = Memory(None if nocache_flag else "./cache", verbose=0)


@memory.cache
def api_fetch_tournaments():
    logger.debug("api fetch tournaments")
    return napi.get_tournaments(only_active=False)


@memory.cache
@sleep_and_retry
@limits(calls=10, period=period)
def api_fetch_leaderboard(round_num, tournament):
    logger.debug("api_fetch_leaderboard {} {}".format(
        round_num, tournament['tournament']))
    arguments = {'number': round_num, 'tournament': tournament['tournament']}
    # FIXME
    q = query if round_num > 100 else query_old
    return napi.raw_query(q, arguments)['data']['rounds']


@memory.cache
def fetch_one(round_num, tournament):
    logger.debug("fetch_one {} {}".format(round_num, tournament['tournament']))
    raw = api_fetch_leaderboard(round_num, tournament)
    if len(raw) > 0:
        df = pd.io.json.json_normalize(raw[0]['leaderboard'], sep='_')
        df.columns = [utils.to_snake_case(col) for col in df.columns]
        df.rename(columns={'payment_staking_nmr_transfer_value': 'nmr_staking',
                           'payment_staking_usd_amount': 'usd_staking',
                           'payment_general_nmr_transfer_value': 'nmr_general',
                           'payment_general_nmr_amount': 'nmr_general_bak',
                           'payment_general_usd_amount': 'usd_general',
                           'return_nmr_amount': 'nmr_returned',
                           'stake_value': 'nmr_staked'},
                  inplace=True)
        df['tournament_num'] = tournament['tournament']
        df['tournament'] = tournament['name']
        df['round_num'] = round_num
        if 'nmr_staked' in df:
            df['nmr_staked'] = df['nmr_staked'].astype(float)
            df['stake_confidence'] = df['stake_confidence'].astype(float)
        else:
            df['nmr_staked'] = np.nan
            df['stake_confidence'] = np.nan
            df['stake_resolution_destroyed'] = np.nan
        df['round_status'] = raw[0]['status']
        if raw[0]['status'] == "RESOLVED":
            df['staking_cutoff'] = raw[0]['selection']['bCutoff']
            df['benchmark_type'] = raw[0]['benchmark_type']
            if 'nmr_staking' in df:
                df['nmr_staking'] = df['nmr_staking'].astype(float)
            if 'usd_staking' in df:
                df['usd_staking'] = df['usd_staking'].astype(float)
            if 'usd_general' in df:
                df['usd_general'] = df['usd_general'].astype(float)
            if 'nmr_general' not in df and 'nmr_general_bak' in df:
                df['nmr_general'] = df['nmr_general_bak']
            if 'nmr_general' in df:
                df['nmr_general'] = df['nmr_general'].astype(float)
            if 'nmr_returned' in df:
                df['nmr_returned'] = df['nmr_returned'].astype(float)
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

            # staking bonus
            if round_num >= 154:
                staking_bonus_perc = 0.05
                bonus_nmr_only = df['nmr_staked'] * staking_bonus_perc
                bonus_split = df['nmr_staking'] - df['nmr_staking'] / (1 + staking_bonus_perc)
                if round_num == 158:
                    df['nmr_bonus'] = bonus_nmr_only.where(df['usd_staking'].isna(), bonus_split)
                    df['usd_bonus'] = df['usd_staking'] - df['usd_staking'] / (1 + staking_bonus_perc)
                    df['usd_staking'] = df['usd_staking'] - df['usd_bonus']
                    df['nmr_staking'] -= df['nmr_bonus']
                    df['nmr_returned'] -= df['nmr_bonus']
                else:
                    df['nmr_bonus'] = bonus_nmr_only

            # partial burns
            if round_num >= 154 and 'nmr_returned' in df:
                df['nmr_burned'] -= df['nmr_returned'].fillna(0)

        return df
    return None


def fetch_leaderboard(round_num):
    dfs = []
    for tournament in api_fetch_tournaments():
        res = fetch_one(round_num, tournament)
        if res is not None:
            dfs.append(res)

    df = pd.concat(dfs, sort=False)
    return df
