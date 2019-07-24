import pandas as pd

from numerai_reports import data


class Leaderboard:

    def __init__(self):
        self.dfs = {}

    def _get(self, round_num):
        if round_num not in self.dfs:
            self.dfs[round_num] = data.fetch_leaderboard(round_num)
        return self.dfs[round_num]

    def __getitem__(self, given):
        if isinstance(given, slice):
            if given.stop is None:
                df = self._get(given.start)
            else:
                step = 1 if given.step is None else given.step
                df = pd.concat(
                    [self._get(i) for i
                     in range(given.start, given.stop + 1, step)], sort=False)
        else:
            df = self._get(given)

        return df
