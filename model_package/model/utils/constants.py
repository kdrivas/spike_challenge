VERSION = "1.0.0"

### Preprocessing
TARGET_COL = "target"

FLOAT_COLS = [
    "popularity",
    "duration",
    "loudness",
    "tempo",
]

SCALE_COLS = ["tempo"]

DROP_COLS = [
    "key",
    "duration",
    "popularity",
    "mode",
    "instrumentalness",
    "loudness",
    "liveness",
    "id_new",
]
