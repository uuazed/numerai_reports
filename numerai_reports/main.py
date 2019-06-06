import numpy as np

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

    return df


if __name__ == "__main__":
    lb = data.fetch_leaderboard(158)

    print(out_of_n(lb))
    print(pass_rate(lb))
    print(all_star_club(lb))
