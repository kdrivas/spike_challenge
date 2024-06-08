import pytest
import pandas as pd
from model.utils.transformers import (
    DropCols
)


@pytest.mark.parametrize(
    "input, drop_cols, expected_output",
    [
        pytest.param(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": [7, 8, 9],
            },
            ["a", "b"],
            [
                {"c": 7}, {"c":  8}, {"c": 9},
            ],
            id="drop_0",
        ),
        pytest.param(
            {
                "a": [1, 2, 3],
                "c": [7, 8, 9],
            },
            ["a"],
            [
                {"c": 7}, {"c":  8}, {"c": 9},
            ],
            id="drop_1",
        )
    ]
)
def test_drop_cols_transformer(
    input: dict, drop_cols: list, expected_output: dict
):
    df_input = pd.DataFrame(input)
    drop = DropCols(cols=drop_cols)

    df_output = drop.fit_transform(df_input)

    assert df_output.to_dict("records") == expected_output
