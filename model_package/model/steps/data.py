"""
    This file contains the function for data gathering, merging and preprocessing
"""
import os
import logging
import pandas as pd
import joblib

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from imblearn.over_sampling import SMOTE

from model.utils.constants import (
    TARGET_COL,
    FLOAT_COLS,
    SCALE_COLS,
    DROP_COLS,
)
from model.utils.transformers import (
    DropCols,
    CastCol,
)

logger = logging.getLogger(__name__)


def validate_assets(base_path: str) -> None:
    pass


def collect_assets(base_path: str, dry_run: bool = False) -> None:
    """
        This function will collect and merge the data from different sources
    """
    df_reggae = pd.read_csv(
        os.path.join(base_path, "data", "raw", "data_reggaeton.csv"), encoding="utf-8",
    )
    df_not_reggae = pd.read_csv(
        os.path.join(base_path, "data", "raw", "data_todotipo.csv"), encoding="utf-8",
    )

    df_reggae[TARGET_COL] = 1
    df_not_reggae[TARGET_COL] = 0

    df_reggae = df_reggae.drop(columns=["Unnamed: 0"])
    df_not_reggae = df_not_reggae.drop(columns=["Unnamed: 0", "time_signature"])

    df = pd.concat((df_reggae, df_not_reggae), axis=0)

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving music data")
        df.to_csv(
            os.path.join(base_path, "data", "interm", "collect_music.csv"), index=False
        )


def preprocess_assets(base_path: str, dry_run: bool = False) -> None:
    """
        This function will execute the data preprocessing and serialize
        the data pipeline
    """
    # The pipeline is divided in two parts due to the presence of null values
    pipe_1 =  Pipeline([
        ("cast_float_cols", CastCol(FLOAT_COLS, "float")),
        ("drop_cols", DropCols(DROP_COLS)),
        ("min_max_scaler", ColumnTransformer(
                [
                    (f"scaler_{col}", MinMaxScaler(), [col]) for col in SCALE_COLS
                ],
                remainder="passthrough",
            ),
        ),
    ])

    # Read the data and set the period as index
    df_merge = pd.read_csv(os.path.join(base_path, "data", "interm", "collect_music.csv"))

    # Running pipeline and removing nulls
    df_prec = pipe_1.fit_transform(df_merge.drop(TARGET_COL, axis=1), df_merge[TARGET_COL])
    df_prec = pd.concat((df_merge[TARGET_COL], pd.DataFrame(df_prec)), axis=1)
    df_prec = df_prec.dropna(how="any", axis=0)

    sm = SMOTE(random_state=0, n_jobs=-1)
    X_res, y_res = sm.fit_resample(
        df_prec.drop(columns=[TARGET_COL]), df_prec[TARGET_COL]
    )
    df_prec_merged = pd.concat((y_res, pd.DataFrame(X_res)), axis=1)

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving artifacts.")

        # Saving data and serializing pipelines
        df_prec_merged.to_csv(
            os.path.join(base_path, "data", "preprocessed", "prec_merged.csv"), index=False
        )
        
        joblib.dump(pipe_1, os.path.join(base_path, "artifacts", "data_pipeline.pkl"))


def create_batch_data(base_path: str, dry_run: bool = False) -> None:
    """
        This function will execute the data preprocessing and create
        the data for running predictions
    """
    # The pipeline is divided in two parts due to the presence of null values
    pipe_1 = joblib.load(os.path.join(base_path, "artifacts", "data_pipeline.pkl"))

    # Read the data and set the period as index
    df_merge = pd.read_csv(os.path.join(base_path, "data", "interm", "collect_music.csv"))

    # Running pipeline and removing nulls
    df_prec = pipe_1.fit_transform(df_merge.drop(TARGET_COL, axis=1), df_merge[TARGET_COL])
    df_prec = pd.DataFrame(df_prec)
    df_prec = df_prec.dropna(how="any", axis=0)

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving artifacts.")

        # Saving data and serializing pipelines
        df_prec.to_csv(
            os.path.join(base_path, "data", "feature_store", "batch_data.csv"), index=False
        )
        
        joblib.dump(pipe_1, os.path.join(base_path, "artifacts", "data_pipeline.pkl"))
