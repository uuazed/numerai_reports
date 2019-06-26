# Changelog
Notable changes to this project.

## [0.2.0] - 2019-06-26
- fix `payments` calculation
- fix crash when fetching old leaderboards
- set start date for reputation bonus
- ensure `payments` report works for older rounds as well

## [0.1.0] - 2019-06-15
- added `summary` report
- added `payments` report
- added `reputation` report
- added `pass_rate` report
- added `out_of_n` report (Fraction of users that get, e.g., 3/5 in a round)
- added `all_star_club` report (Users who beat benchmark in all active tournaments)
- added `dominance` report
- code to fetch numerai leaderboards as pandas DataFrames
- caching
- setup testing
- fixed reputation bonus calculation
- tests: mock api requests
- tests: diable caching
- tests: setup travis & codecov
