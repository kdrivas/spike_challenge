"""
    This file contains the function for data gathering, merging and preprocessing
"""
import os
import pandas as pd
import joblib

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from imblearn.over_sampling import SMOTE

from pipelines.music_model.constants import (
    TARGET_COL,
    FLOAT_COLS,
    SCALE_COLS,
    DROP_COLS,
)
from utils.transformers import (
    DropCols,
    CastCol,
)


########### Data Gathering
def collect_music(path: str) -> None:
    """
        This function will collect and merge the data from different sources
    """
    df_reggae = pd.read_csv(
        os.path.join(path, "raw", "data_reggaeton.csv"), encoding="utf-8",
    )
    df_not_reggae = pd.read_csv(
        os.path.join(path, "raw", "data_todotipo.csv"), encoding="utf-8",
    )

    df_reggae[TARGET_COL] = 1
    df_not_reggae[TARGET_COL] = 0

    df_reggae = df_reggae.drop(columns=["Unnamed: 0"])
    df_not_reggae = df_not_reggae.drop(columns=["Unnamed: 0", "time_signature"])

    df = pd.concat((df_reggae, df_not_reggae), axis=0)

    df.to_csv(os.path.join(path, "interm", "collect_music.csv"), index=False)


########### Data Preprocessing
def preprocess_data(data_path: str, artifact_path: str) -> None:
    """
        This function will execute the data preprocessing and serialize the data pipeline
    """
    # The pipeline is divided in two parts due to the presence of null values
    pipe_1 =  Pipeline([
        ("cast_float_cols", CastCol(FLOAT_COLS, "float")),
        ("drop_cols", DropCols(DROP_COLS)),
        ("min_max_scaler", ColumnTransformer(
                [(f"scaler_{col}", MinMaxScaler(), [col]) for col in SCALE_COLS],
                remainder="passthrough"
            ),
        ),
    ])

    # Read the data and set the period as index
    df_merge = pd.read_csv(os.path.join(data_path, "interm", "collect_music.csv"))

    # Running pipeline and removing nulls
    df_prec = pipe_1.fit_transform(df_merge.drop(TARGET_COL, axis=1), df_merge[TARGET_COL])
    df_prec = pd.concat((df_merge[TARGET_COL], pd.DataFrame(df_prec)), axis=1)
    df_prec = df_prec.dropna(how="any", axis=0)

    sm = SMOTE(random_state=0, n_jobs=-1)
    X_res, y_res = sm.fit_resample(
        df_prec.drop(columns=[TARGET_COL]), df_prec[TARGET_COL]
    )
    df_prec_merged = pd.concat((y_res, pd.DataFrame(X_res)), axis=1)

    # Saving data and serializing pipelines
    df_prec_merged.to_csv(os.path.join(data_path, "preprocessed", "prec_merged.csv"), index=False)
    
    joblib.dump(pipe_1, os.path.join(artifact_path, "data_pipeline.pkl"))
