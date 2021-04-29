from functools import lru_cache
import os

import pandas as pd
import numerapi
import tqdm

import utils
import settings


napi = numerapi.NumerAPI(verbosity='warn')


@lru_cache(maxsize=None)
def fetch_one_model(username="uuazed"):
    query = '''
        query($username: String!) {
          v2UserProfile(username: $username) {
            accountId
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
          }
        }
    '''
    arguments = {'username': username}
    raw = napi.raw_query(query, arguments)['data']['v2UserProfile']
    df = pd.DataFrame(raw['latestRoundPerformances'])
    df['model'] = username
    df['account'] = raw['accountId']
    df['id'] = raw['id']
    raw['medals']['model'] = username
    return df, raw['medals']


def fetch_leaderboard(limit=10000):
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


def fetch_from_api():
    leaderboard = fetch_leaderboard()
    models = leaderboard['model'].to_list()
    dfs = [fetch_one_model(model) for model in tqdm.tqdm(models)]
    df = pd.concat([details for details, _ in dfs])
    medals = pd.DataFrame.from_records([med for _, med in dfs])
    df.dropna(subset=['correlation'], inplace=True)
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
    df['account'] = df['account'].map(account_names)
    df.drop(columns=['id', 'leaderboard_bonus'], inplace=True)

    # add account information to leaderboard
    account_map = df.set_index('model')['account'].to_dict()
    leaderboard['account'] = leaderboard['model'].map(account_map)
    leaderboard = leaderboard.merge(medals, on="model", how="left")

    return df, leaderboard


def fetch_from_cloud():
    try:
        leaderboard = pd.read_parquet(
            os.path.join("gs://" + settings.CLOUD_BUCKET, "leaderboard.parq"))
        details = pd.read_parquet(
            os.path.join("gs://" + settings.CLOUD_BUCKET, "details.parq"))
        return details, leaderboard
    except FileNotFoundError:
        return None, None


def load():
    details, leaderboard = fetch_from_cloud()
    if details is None or leaderboard is None:
        details, leaderboard = fetch_from_api()
    return details, leaderboard
