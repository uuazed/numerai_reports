import data

details, leaderboard = data.load()


def most_top_models(rank_limit=100, metric="corr"):
    if metric not in {'corr', 'fnc', 'mmc'}:
        raise ValueError(f"invalid metric {metric}")
    agg = {"corr_rep": "count", "model": lambda g: ", ".join(sorted(list(g)))}
    subset = leaderboard.sort_values(f'{metric}_rep', ascending=False)\
                        .head(rank_limit)
    grouped = subset.groupby("account").agg(agg)
    grouped.rename(columns={"corr_rep": "#models", "model": "models"},
                   inplace=True)
    grouped = grouped[grouped['#models'] > 1]
    return grouped.sort_values("#models", ascending=False)


def all_positive(rounds=20, metric="corr"):
    "models having positive 'metric' in the each of the last 'rounds' rounds"
    if metric not in {'corr', 'fnc', 'mmc'}:
        raise ValueError(f"invalid metric {metric}")
    subset = details[details['round'] > details['round'].max() - rounds].copy()
    subset['success'] = subset[metric] >= 0

    grouped = subset.groupby('model').agg({"success": "sum", metric: "mean"})
    grouped = grouped[grouped['success'] == rounds]
    grouped.sort_values(metric, ascending=False, inplace=True)
    grouped.rename(columns={metric: f'mean_{metric}'}, inplace=True)
    return grouped[f'mean_{metric}']


def all_star_club(top_n=100):
    cols = ['model', 'account', "corr_rank", "corr_rep",
            "mmc_rank", "mmc_rep", "fnc_rank", "fnc_rep"]
    res = leaderboard.loc[
        (leaderboard['corr_rank'] <= top_n) &
        (leaderboard['mmc_rank'] <= top_n) &
        (leaderboard['fnc_rank'] <= top_n)][cols].copy()
    return res


def payouts(models, groupby="round"):
    """aggregated payouts for a list of models"""
    subset = details[details['model'].isin(models)].copy()
    result = subset.groupby(groupby)['payout'].sum()
    return result


def medals_leaderboard(limit=10, orderby="total"):
    cols = ['gold', 'silver', 'bronze']
    lb = leaderboard.groupby("account")[cols].sum()
    lb['total'] = lb.sum(axis=1)
    lb.sort_values(orderby, ascending=False, inplace=True)
    return lb.head(limit)


def models_of_account(model):
    """return all models that belong the same account given a model name"""
    account = leaderboard[leaderboard['model'] == model]['account'].iloc[0]
    res = leaderboard[leaderboard['account'] == account]['model'].unique()
    return list(res)
