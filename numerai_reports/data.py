import pandas as pd
import numerapi
import tqdm

import utils


query = '''
    query($number: Int!
          $tournament: Int!) {
      rounds(number: $number
             tournament: $tournament) {
        leaderboard {
          liveAuroc
          username
          validationAuroc
          paymentGeneral {
            nmrAmount
            usdAmount
          }
          paymentStaking {
            nmrAmount
            usdAmount
          }
          stake {
            value
            confidence
          }
        }
      }
    }
'''


def fetch_leaderboard(start=0, end=None):
    napi = numerapi.NumerAPI(verbosity='warn')

    dfs = []
    rounds = [start] if end is None else range(start, end + 1)

    for round_num in tqdm.tqdm(rounds):
        for tournament in napi.get_tournaments():
            tournament_num = tournament['tournament']
            arguments = {'number': round_num, 'tournament': tournament_num}
            raw = napi.raw_query(query, arguments)['data']['rounds']
            leaderboard = [utils.flatten(d) for d in raw[0]['leaderboard']]
            df = pd.DataFrame(leaderboard)
            df['tournament_num'] = tournament_num
            df['tournament'] = tournament['name']
            df['round_num'] = round_num
            df['staking_cutoff'] = str(napi.get_staking_cutoff(
                round_num, tournament_num))
            dfs.append(df)
    df = pd.concat(dfs, sort=False)
    df.columns = [utils.to_snake_case(col) for col in df.columns]

    df['pass'] = (df['live_auroc'] > 0.5).astype(int)

    return df
