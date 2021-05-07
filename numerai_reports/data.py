import os
import logging
from typing import Tuple, Optional, List

import pandas as pd
import numerapi
import tqdm

from numerai_reports import utils
from numerai_reports import settings


napi = numerapi.NumerAPI(verbosity='warn')
logger = logging.getLogger(__name__)
NO_CLOUD_BuCKET = os.environ.get('NO_CLOUD_BuCKET', False)


def fetch_models(models: List[str],
                 batch_size: int = 100) -> Tuple[pd.DataFrame, pd.DataFrame]:
    dfs = []
    medals = []
    for chunk in tqdm.tqdm(utils.chunks(models, batch_size)):
        model_queries = [f'_{i} : v2UserProfile(username : "{m}"){{...f}}'
                         for i, m in enumerate(chunk)]
        query = '{' + '\n'.join(model_queries) + """}
            fragment f on V2UserProfile {
                accountId
                username
                id
                medals {
                  gold
                  silver
                  bronze
                }
                latestRoundPerformances {
                  correlation
                  date
                  fnc
                  mmc
                  correlationWithMetamodel
                  payoutPending
                  roundNumber
                  selectedStakeValue
                  leaderboardBonus
                }
            }"""
        raw = napi.raw_query(query)['data']
        for _, vals in raw.items():
            if vals is None:
                continue
            df = pd.DataFrame(vals['latestRoundPerformances'])
            df['model'] = vals['username']
            df['account'] = vals['accountId']
            df['id'] = vals['id']
            vals['medals']['model'] = vals['username']
            dfs.append(df)
            medals.append(vals['medals'])

    # put everything together
    df = pd.concat(dfs)
    medals = pd.DataFrame.from_records(medals)
    return df, medals


def fetch_leaderboard(limit: int = 10000) -> pd.DataFrame:
    query = '''
        query($limit: Int
              $offset: Int) {
          v2Leaderboard(limit: $limit
                        offset: $offset) {
            username
            mmcRep
            corrRep
            fncRep
            rank
            fncRank
            mmcRank
          }
        }
    '''
    arguments = {'limit': limit, 'offset': 0}
    raw = napi.raw_query(query, arguments)
    df = pd.DataFrame(raw['data']['v2Leaderboard'])
    df.rename(columns={'username': 'model',
                       'rank': 'corr_rank'}, inplace=True)
    df.columns = [utils.to_snake_case(col) for col in df.columns]
    return df


def fetch_from_api() -> Tuple[pd.DataFrame, pd.DataFrame]:
    leaderboard = fetch_leaderboard()
    df, medals = fetch_models(leaderboard['model'].to_list())
    df.rename(columns={'correlation': 'corr',
                       'correlationWithMetamodel': "corr_with_mm",
                       'roundNumber': 'round',
                       'selectedStakeValue': 'stake',
                       'payoutPending': 'payout'}, inplace=True)
    df.columns = [utils.to_snake_case(col) for col in df.columns]
    df['stake'] = df['stake'].astype("float")
    df['payout'] = (df['payout'].astype("float") +
                    df['leaderboard_bonus'].astype("float"))
    df['date'] = pd.to_datetime(df['date']).dt.date

    # handle account / model information
    account_names = df[df['id'] == df['account']].set_index('account')['model']
    account_names = account_names.drop_duplicates().to_dict()
    df['account'] = df['account'].map(account_names).fillna(df['account'])
    df.drop(columns=['id', 'leaderboard_bonus'], inplace=True)

    # add account information to leaderboard
    account_map = df.set_index('model')['account'].to_dict()
    leaderboard['account'] = leaderboard['model'].map(account_map)
    leaderboard = leaderboard.merge(medals, on="model", how="left")

    df.dropna(subset=['corr'], inplace=True)

    return df, leaderboard


def fetch_from_cloud() -> Tuple[Optional[pd.DataFrame],
                                Optional[pd.DataFrame]]:
    try:
        leaderboard = pd.read_parquet(
            os.path.join("gs://" + settings.CLOUD_BUCKET, "leaderboard.parq"))
        details = pd.read_parquet(
            os.path.join("gs://" + settings.CLOUD_BUCKET, "details.parq"))
        return details, leaderboard
    except FileNotFoundError:
        return None, None


class Data(metaclass=utils.Singleton):

    def __init__(self):
        self._details = None
        self._leaderboard = None

    @property
    def leaderboard(self):
        if self._leaderboard is None:
            self.load()
        return self._leaderboard

    @property
    def details(self):
        if self._details is None:
            self.load()
        return self._details

    def load(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if not NO_CLOUD_BuCKET:
            details, leaderboard = fetch_from_cloud()
        else:
            details, leaderboard = None, None
        if details is None or leaderboard is None:
            details, leaderboard = fetch_from_api()
        self._details = details
        self._leaderboard = leaderboard
        return details, leaderboard
