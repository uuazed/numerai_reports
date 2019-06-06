import pandas as pd
import numerapi

import utils


def fetch_leaderboard(start=0, end=None):
    napi = numerapi.NumerAPI(verbosity='warn')

    dfs = []
    rounds = [start] if end is None else range(start, end + 1)

    for round_num in rounds:
        for tournament in napi.get_tournaments():
            tournament_num = tournament['tournament']
            leaderboard = napi.get_leaderboard(round_num, tournament_num)
            leaderboard = [utils.flatten(d) for d in leaderboard]
            df = pd.DataFrame(leaderboard)
            df['tournament_num'] = tournament_num
            df['tournament'] = tournament['name']
            df['round_num'] = round_num
            dfs.append(df)
    df = pd.concat(dfs)

    df['pass'] = df['liveAuroc'] > 0.5

    return df
