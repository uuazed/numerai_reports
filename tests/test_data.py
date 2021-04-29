import pytest
import pandas as pd

from numerai_reports import data

from . import napi


def test_fetch_one_model(napi):
    # normal behaviour
    df, _ = data.fetch_one_model("one")
    assert isinstance(df, pd.DataFrame)

    # unknown user
    with pytest.raises(ValueError) as excinfo:
        print(data.fetch_one_model("not_available"))

    assert "unknown model" in str(excinfo.value)
