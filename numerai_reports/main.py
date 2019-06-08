import collections
import decimal

import numpy as np
import pandas as pd

import data


def all_star_club(lb):
    "Users who beat benchmark in all active tournaments sorted by mean auroc"
    df = lb.groupby('username').agg({"pass": "sum", "live_auroc": "mean"})
    df = df[df['pass'] == lb['tournament'].nunique()]
    df = df[['live_auroc']].rename(columns={'live_auroc': 'mean_auroc'})
    df.sort_values('mean_auroc', ascending=False, inplace=True)
    return df


def out_of_n(lb):
    "Fraction of users that get, e.g., 3/5 in a round"
    df = lb.groupby(['round_num', 'username'], as_index=False)['pass'].sum()
    means_per_round = df.groupby(["round_num"])['pass'].mean().round(3)
    df = df.groupby(["round_num", "pass"], as_index=False).count()
    df = df.pivot("round_num", "pass", "username")

    pass_columns = [i for i in range(lb['tournament'].nunique() + 1)]
    df.columns = pass_columns

    # number of usrs per round
    df['N'] = df.sum(axis=1)
    # average succesful tournaments per user
    df['mean'] = means_per_round
    # convert to fractions
    for col in pass_columns:
        df[col] /= df['N']
    # summary row
    df.loc['mean', :] = df.mean(axis=0)
    # adjust dtypes
    df['N'] = df['N'].round().astype(int)
    return df


def pass_rate(lb):
    df = lb.copy()
    df['has_staked'] = df['stake_value'].notnull()
    df['all'] = df['pass']
    df['stakers'] = df['pass'].where(df['has_staked'], np.nan)
    df['non_stakers'] = df['pass'].where(~df['has_staked'], np.nan)
    df['above_cutoff'] = df['pass'].where(
        df['stake_confidence'] >= df['staking_cutoff'], np.nan)
    df['below_cutoff'] = df['pass'].where(
        df['stake_confidence'] < df['staking_cutoff'], np.nan)
    cols = ['all', 'stakers', 'non_stakers', 'above_cutoff', 'below_cutoff']
    df = df.groupby('round_num')[cols].mean()
    # summary row
    df.loc['mean', :] = df.mean(axis=0)
    return df


def reputation(lb, users, window_size=20, fill=0.4):
    first_round = lb['round_num'].min()
    last_round = lb['round_num'].max()
    res = collections.defaultdict(dict)

    for start in range(first_round, last_round + 1):
        end = min(start + window_size - 1, last_round)
        subset = lb[lb.round_num.between(start, end)]
        n_tournaments = len(subset.groupby(['tournament', 'round_num']).sum())
        key = "{}-{}".format(start, end)
        if end - start < window_size - 1:
            key += "*"
        for user in users:
            res[user][key] = {}
            aurocs = subset[subset['username'] == user]['live_auroc'].tolist()
            missing = [fill] * (n_tournaments - len(aurocs))
            aurocs += missing
            reputation = np.mean(aurocs)
            res[user][key] = reputation

    df = pd.DataFrame(res)
    return df


def payments(lb, users):
    if not isinstance(users, list):
        users = [users]
    df = lb[lb['username'].isin(users)]

    # FIXME we assume everyone gets a staking bonus
    reps = []
    for round_num in df['round_num'].unique().tolist():
        if round_num >= 158:
            df_rep = data.fetch_leaderboard(round_num - 19)
            df_rep = df_rep[df_rep['username'].isin(users)]
            bonus = df_rep['nmr_staked'].sum() * decimal.Decimal('0.5')
        else:
            df_rep = df[df['round_num'] == round_num]
            bonus = (df_rep['pass'] * decimal.Decimal('0.1')).sum()
        reps.append(((bonus, round_num)))
    reps = pd.DataFrame(reps, columns=['nmr_rep_bonus', 'round_num'])

    cols = ['nmr_staked', 'nmr_burned', 'nmr_staking', 'usd']
    df = df.groupby('round_num')[cols].sum()
    df = df.merge(reps, how="left", on="round_num")
    df.set_index("round_num", inplace=True)
    df['nmr_total'] = df['nmr_staking'] - df['nmr_burned'] + df['nmr_rep_bonus']
    # summary row
    df.loc['total', :] = df.sum(axis=0)
    return df


if __name__ == "__main__":
    lb = data.fetch_leaderboard(150, 158)

    print(reputation(lb, ['uuazed', 'uuazed2', 'uuazed3']))
    print(payments(lb, 'uuazed2'))
