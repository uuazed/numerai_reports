from __future__ import division

import collections

import numpy as np
import pandas as pd

from numerai_reports import leaderboard


lb = leaderboard.Leaderboard()


def all_star_club(round_num):
    "Users who beat benchmark in all active tournaments sorted by mean auroc"
    df = lb[round_num]
    num_tournaments = df['tournament'].nunique()
    df = df.groupby('username').agg({"pass": "sum", "live_auroc": "mean"})
    df = df[df['pass'] == num_tournaments]
    df = df[['live_auroc']].rename(columns={'live_auroc': 'mean_auroc'})
    df.sort_values('mean_auroc', ascending=False, inplace=True)
    return df


def out_of_n(round_start, round_end=None):
    "Fraction of users that get, e.g., 3/5 in a round"
    df = lb[round_start: round_end]
    num_tournaments = df['tournament'].nunique()
    df = df.groupby(['round_num', 'username'], as_index=False)['pass'].sum()
    means_per_round = df.groupby(["round_num"])['pass'].mean().round(3)
    df = df.groupby(["round_num", "pass"], as_index=False).count()
    df = df.pivot("round_num", "pass", "username")

    pass_columns = [i for i in range(num_tournaments + 1)]
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


def pass_rate(round_start, round_end=None):
    df = lb[round_start: round_end]
    df['has_staked'] = df['nmr_staked'].notnull()
    df['all'] = df['pass']
    df['stakers'] = df['pass'].where(df['has_staked'], np.nan)
    df['stakers>0.1'] = df['pass'].where(df['nmr_staked'] > 0.1, np.nan)
    df['stakers<=0.1'] = df['pass'].where(df['nmr_staked'] <= 0.1, np.nan)
    df['non_stakers'] = df['pass'].where(~df['has_staked'], np.nan)
    df['above_cutoff'] = df['pass'].where(
        df['stake_confidence'] >= df['staking_cutoff'], np.nan)
    df['below_cutoff'] = df['pass'].where(
        df['stake_confidence'] < df['staking_cutoff'], np.nan)
    cols = ['all', 'stakers', 'stakers>0.1', 'stakers<=0.1', 'non_stakers',
            'above_cutoff', 'below_cutoff']
    df = df.groupby('round_num')[cols].mean()
    # summary row
    df.loc['mean', :] = df.mean(axis=0)

    df = df.round(3)
    return df


def _calc_reputation(round_num, window_size=20):
    first_round = round_num - window_size + 1
    df = lb[first_round:round_num]
    df['stake'] = df['nmr_staked'].where(df['round_num'] == first_round, 0)

    tourneys = df[['round_num', 'tournament']].drop_duplicates()
    users = df[['username']].drop_duplicates()
    tourneys['tmp'] = 1
    users['tmp'] = 1
    users_tourneys = users.merge(tourneys, on='tmp')
    del users_tourneys['tmp']
    df = users_tourneys.merge(
        df, on=["round_num", "tournament", "username"], how="left")

    agg = {'stake': "sum", "mu": "mean"}

    if round_num < 164:
        # fill tournaments user's haven't participated in
        df['mu'] = df['live_auroc'].fillna(0.4)
        # everything is equally weighted
        df = df.groupby("username").agg(agg)
    else:
        # fill tournaments user's haven't participated in
        # use correlation score
        # use auc-0.5 for rounds before 168
        df["mu"] = np.where(
            df['round_num'] >= 168,
            df['live_correlation'],
            df['live_auroc'] - 0.5)
        df['mu'] = df['mu'].fillna(-0.1)
        # rounds are weighted equally
        df = df.groupby(["username", "round_num"], as_index=False).agg(agg)
        df = df.groupby("username").agg(agg)

    return df


def reputation(users, round_start, round_end=None, window_size=20, rank=False):
    if not isinstance(users, list):
        users = [users]
    if round_end is None:
        round_end = round_start
    dfs = []

    for round_num in range(round_start, round_end + 1):
        df = _calc_reputation(round_num, window_size).reset_index()
        if rank:
            df['mu'] = df['mu'].rank(ascending=False).astype(int)
        df = df[df['username'].isin(users)]
        key = "{}-{}".format(round_num - window_size + 1, round_num)
        if lb[round_num]["round_status"].iloc[0] != "RESOLVED":
            key += "*"
        df["key"] = key
        del df['stake']
        dfs.append(df)

    df = pd.concat(dfs)
    df = df.pivot("key", "username", "mu")
    return df


def reputation_bonus(round_num, window_size=20):

    df = _calc_reputation(round_num, window_size)

    df = df[df['stake'] > 0]
    df.sort_values("mu", inplace=True, ascending=False)
    df['cumstake'] = df['stake'].cumsum()
    df['selected'] = np.minimum(
        df['stake'],
        (1000 - df['cumstake'].shift(fill_value=0)).clip(lower=0))
    df['bonus'] = df['selected'] * 0.5
    return df


def payments(users, round_start, round_end=None):
    if not isinstance(users, list):
        users = [users]
    df = lb[round_start:round_end]
    df = df[df['username'].isin(users)]

    reps = []
    for round_num in df['round_num'].unique().tolist():
        if round_num >= 158:
            df_rep = reputation_bonus(round_num)
            df_rep = df_rep[df_rep.index.isin(users)]
            bonus = df_rep['bonus'].sum()
        # FIXME verify start round of 0.1 NMR bonus
        elif round_num >= 101:
            df_rep = df[df['round_num'] == round_num]
            bonus = (df_rep['pass'] * 0.1).sum()
        else:
            bonus = 0
        reps.append(((bonus, round_num)))
    reps = pd.DataFrame(reps, columns=['nmr_rep_bonus', 'round_num'])

    cols = list({'nmr_staked', 'nmr_burned', 'nmr_staking', 'nmr_bonus',
                 'nmr_general', 'usd_bonus', 'usd_staking',
                 'usd_general'} & set(df.columns))
    df = df.groupby('round_num')[cols].sum()
    df = df.merge(reps, how="left", on="round_num")
    df.set_index("round_num", inplace=True)

    cols = [c for c in df.columns
            if c.startswith("nmr") and c != "nmr_burned" and c != "nmr_staked"]
    df['nmr_total'] = df[cols].sum(axis=1)
    if 'nmr_burned' in df:
        df['nmr_total'] -= df['nmr_burned']
    cols = [c for c in df.columns if c.startswith("usd")]
    df['usd_total'] = df[cols].sum(axis=1)
    # summary row
    df.loc['total', :] = df.sum(axis=0)
    df = df.round(2)
    return df


def friends(user, round_start, round_end=None, metric="live_auroc"):
    """Correlation of live auroc of each user to a given `user`"""
    df = lb[round_start: round_end]
    df = df[['username', 'round_num', metric, 'tournament']]
    df['round_tournament'] = df['round_num'].astype(str) + df['tournament']
    df = df.set_index('username')
    df = df.pivot(columns='round_tournament', values=metric)
    df = df.dropna()
    corr = df.T.corr()
    df = corr.loc[user]
    df = df.sort_values(ascending=False)
    df = df.to_frame('mean_correlation')
    df = df.drop(user, axis=0)
    return df


def dominance(user, round_start, round_end=None,
              kpi="live_auroc", direction='more'):
    "Fraction of users that `user` beats in terms of 'kpi'"
    df = lb[round_start: round_end]
    df_user = df.loc[df['username'] == user]
    df_user = df_user[['round_num', 'tournament', kpi]]
    df_user.rename(columns={kpi: 'user_' + kpi}, inplace=True)
    df_others = df.loc[df['username'] != user]
    df = df_others.merge(df_user, on=['round_num', 'tournament'], how="left")

    if direction == 'more':
        df['dominated'] = df[kpi] < df['user_' + kpi]
    elif direction == 'less':
        df['dominated'] = df[kpi] > df['user_' + kpi]
    else:
        raise ValueError(direction)
    df['dominated'] = df['dominated'].where(df['user_' + kpi].notna(), np.nan)
    cols = ['round_num', 'tournament']
    df = df.groupby(cols)['dominated'].agg(['count', 'sum'])
    df['frac'] = df['sum'] / df['count']
    df = df.reset_index().pivot('round_num', 'tournament', 'frac')
    # summary row & column
    df.loc[:, 'mean'] = df.mean(axis=1)
    df.loc['mean', :] = df.mean(axis=0)

    return df


def summary(round_start, round_end=None):
    df = lb[round_start: round_end]
    df['cutoff'] = df['staking_cutoff'].astype(float)
    df = df.groupby('round_num').agg({'pass': 'mean',
                                      'username': 'nunique',
                                      'tournament': 'count',
                                      'cutoff': 'mean'})
    df.rename(columns={'pass': 'pass_rate',
                       'username': 'users',
                       'tournament': 'submissions'}, inplace=True)
    df['tourneys/user'] = (df['submissions'] / df['users']).round(2)

    df = df.round(2)

    return df
