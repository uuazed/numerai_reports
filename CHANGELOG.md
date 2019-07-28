# Changelog
Notable changes to this project.

## [dev]
- added log of tournament changes (`settings.events`)
- use event log instead of magic numbers
- increase test coverage to >90%

## [0.3.3 - 2019-07-27]
- `reputation` report gained a `rank` option, that allows showing the
  leaderboard position instead of raw reputation score
- only try fetching rounds/tournaments that actually exist to reduce load
  on numerai's backend
- ensure cached leaderboards are invalidated when their status changes
- notify about leaderboard fetches, to avoid the app to appear as hanging..
- update example code
- update tests

## [0.3.2] - 2019-07-25
- fix import problems

## [0.3.1] - 2019-07-24
- fix `reputation` calculation

## [0.3.0] - 2019-07-24
- fix `reputation` report
- `reputation_bonus` is using correlation_score for rounds >= 164
- `reputation_bonus` is using weighted averaging for rounds >= 164
- rate limit requests to the numerai api
- introduce `Leaderboard` class to abstract leaderboard fetching
- allow turning off rate limiting
- test: increase coverage

## [0.2.3] - 2019-06-27
- added `friends` report - showing correlation of a user's live auroc to other users
- fix `payments` report for rounds >= 158

## [0.2.2] - 2019-06-26
- create cache directory in current working directory

## [0.2.1] - 2019-06-26
- added `reputation_bonus` report

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
