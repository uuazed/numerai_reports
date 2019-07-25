[![Build Status](https://travis-ci.org/uuazed/numerai_reports.png)](https://travis-ci.org/uuazed/numerai_reports)
[![codecov](https://codecov.io/gh/uuazed/numerai_reports/branch/master/graph/badge.svg)](https://codecov.io/gh/uuazed/numerai_reports)
[![Requirements Status](https://requires.io/github/uuazed/numerai_reports/requirements.svg?branch=master)](https://requires.io/github/uuazed/numerai_reports/requirements/?branch=master)

# Numerai Reports
Collection of reports about the numer.ai machine learning competition.

This library was created to provide some reports and statistics about the
competition, like round summaries and payout summaries, but also tools to
analyze one's model performance. For now, all reports are pure numeric reports.

All information is retrieved via numerai's API and converted to pandas
DataFrames, to make it easy to work with. This allows to create your own
reports on top. `numerai_reports` also caches API results to disk to limit the
amount of request and to speed-up report generation.

If you encounter a problem or have suggestions, feel free to open an issue.

# Installation
`pip install --upgrade numerai_reports`

# Usage

Numerai_reports can be used as a regular, importable Python module or from the command
line (TODO).


## Python module

Usage example:

    from numerai_reports import reports
    print(reports.payments("slyfox", 160, 165))
    print(reports.reputation("slyfox", 160, 165))
    print(reports.pass_rate(160, 165))
    print(reports.out_of_n(160, 165))
    print(reports.dominance("uuazed", 160, 165))
