import warnings
import numpy as np 
import pandas as pd
import data

def get_cross_sectional_factors(start, end, apt_weight=50):
    cross_sectional_data = data.get_cross_sectional_data(start, end)
    T, N = cross_sectional_data.shape
    X = cross_sectional_data.values
    (X.T @ X) / T + apt_weight * (np.mean(X)


