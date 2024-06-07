from typing import Callable, Dict

import fire
import logging
import logging.config

from model.steps.data import (
    validate_assets,
    collect_assets,
    preprocess_assets,
    create_batch_data,
)
from model.steps.model import (
    training_model,
    run_batch_prediction,
)

tasks: Dict[str, Callable] = {
    "validate_assets": validate_assets,  # (1)
    "collect_assets": collect_assets,  # (2)
    "preprocess_assets": preprocess_assets,  # (2)
    "training_model": training_model,
    "create_batch_data": create_batch_data,
    "run_batch_prediction": run_batch_prediction,
}


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
        level=logging.DEBUG,
    )

    fire.Fire(tasks)
