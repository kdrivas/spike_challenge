"""
    This file contains functions for model training and validation
"""
import os
import logging
import joblib
import json
import pandas as pd

from model.utils.constants import TARGET_COL
from sklearn.ensemble import GradientBoostingClassifier

logger = logging.getLogger(__name__)


def training_model(base_path: str, dry_run: bool = False) -> None:
    """
        This function will train the model using the preprocessed data
        Once the model is trained, the serialized model is stored in the artifact_path
    """
    train = pd.read_csv(os.path.join(base_path, "data", "preprocessed", "prec_merged.csv"))

    with open(os.path.join(base_path, "artifacts", "best_params.json")) as f:
        params = json.load(f)

    # Prediction pipeline
    # I"d prefer to keep just the model
    classifier_GB = GradientBoostingClassifier(**params)

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]

    model = classifier_GB.fit(X_train, y_train)

    if dry_run:
        logger.info("Skipping saving")
    else:
        logger.info("Saving model")
        joblib.dump(
            model, os.path.join(base_path, "artifacts", "model.pkl")
        )


def batch_prediction_model(base_path: str) -> None:
    """
        Predict batch data
    """

    # Read historic features from our feature store
    features = pd.read_csv(
        os.path.join(base_path, "feature_store", "batch_data.csv")
    )

    model = joblib.load(os.path.join(base_path, "artifacts", "model.pkl"))

    pred = model.predict(features)
    features["preds"] = pred

    features.to_csv(os.path.join(base_path, "batch_predictions", "predictions.csv"), index=False)
