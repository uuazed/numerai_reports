from typing import List
import logging

import pandas as pd

from numerai_reports.data import Data


logger = logging.getLogger(__name__)


def all_positive(rounds: int = 20, metric: str = "corr") -> pd.DataFrame:
    "models having positive 'metric' in the each of the last 'rounds' rounds"
    if metric not in {'corr', 'fnc', 'mmc'}:
        raise ValueError(f"invalid metric {metric}")
    df = Data().details
    subset = df[df['round'] > df['round'].max() - rounds].copy()
    subset['success'] = subset[metric] >= 0

    grouped = subset.groupby('model').agg({"success": "sum", metric: "mean"})
    grouped = grouped[grouped['success'] == rounds]
    grouped.sort_values(metric, ascending=False, inplace=True)
    grouped.rename(columns={metric: f'mean_{metric}'}, inplace=True)
    return grouped[f'mean_{metric}']


def all_star_club(top_n: int = 100) -> pd.DataFrame:
    cols = ['model', 'account', "corr_rank", "corr_rep",
            "mmc_rank", "mmc_rep", "fnc_rank", "fnc_rep"]
    lb = Data().leaderboard
    res = lb.loc[
                (lb['corr_rank'] <= top_n) &
                (lb['mmc_rank'] <= top_n) &
                (lb['fnc_rank'] <= top_n)][cols].copy()
    return res


def payouts(models: List[str], groupby: str = "round") -> pd.DataFrame:
    """aggregated payouts for a list of models"""
    df = Data().details
    subset = df[df['model'].isin(models)].copy()
    result = subset.groupby(groupby)['payout'].sum()
    return result


def medals_leaderboard(limit: int = 10,
                       orderby: str = "total") -> pd.DataFrame:
    cols = ['gold', 'silver', 'bronze']
    lb = Data().leaderboard.groupby("model")[cols].sum()
    lb['total'] = lb.sum(axis=1)
    lb.sort_values(orderby, ascending=False, inplace=True)
    return lb.astype(int).fillna(0).head(limit)


def models_of_account(model: str) -> List[str]:
    """return all models that belong the same account given a model name"""
    lb = Data().leaderboard
    try:
        account = lb[lb['model'] == model]['account'].iloc[0]
    except IndexError:
        logger.warning(f"Unknown model {model}. Check spelling.")
        return []
    res = lb[lb['account'] == account]['model'].unique()
    return list(res)
