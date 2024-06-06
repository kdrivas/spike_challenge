"""
    This file contains the function for data gathering, merging and preprocessing
"""
import os
import pandas as pd


########### Data Gathering
def collect_music(path: str) -> None:
    """
        This function will collect and merge the data from different sources
    """
    df_reggae = pd.read_csv(
        os.path.join(path, "raw/data_reggaeton.csv"), encoding="utf-8",
    )
    df_not_reggae = pd.read_csv(
        os.path.join(path, "raw/data_todotipo.csv"), encoding="utf-8",
    )

    df_reggae["target"] = 1
    df_not_reggae["target"] = 0

    df_reggae = df_reggae.drop(columns=["Unnamed: 0"])
    df_not_reggae = df_not_reggae.drop(columns=["Unnamed: 0"])

    df = pd.concat((df_reggae, df_not_reggae), axis=0)

    df.to_csv(os.path.join(path, "interm/collect_music.csv"), index=False)
