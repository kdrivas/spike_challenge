"""
    This file contains transformer classes for scikit learn pipelines.
    The intention is to reuse the code
"""
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class DropCols(BaseEstimator, TransformerMixin):
    """
        Drop columns from the given list
    """
    def __init__(self, cols: list=[]):
        self.cols = cols
    
    def fit(self, X: pd.DataFrame, y=None):
        return self
    
    def transform(self, X: pd.DataFrame):
        X = X.drop(columns=self.cols)
        return X


class CastCol(BaseEstimator, TransformerMixin):
    """
        Casting transformer
    """
    def __init__(self, cols: list, cols_type: str):
        self.cols = cols
        self.cols_type = cols_type

    def fit(self, X: pd.DataFrame, y=None):
        return self

    def transform(self, X: pd.DataFrame):
        X = X.copy()
        for col in self.cols:
            X[col] = X[col].astype(self.cols_type)
        return X
