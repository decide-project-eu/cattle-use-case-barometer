import hashlib

import pandas as pd


def hashing_function(variable):
    """Hash a variable using the sha256 hashing algorithm.

    A simple function which calculates the sha256 hash of a given variable and
    returns this hash. Useful for i.e. anonymising variables. The hash of only
    calculated when the provided variable is not null, otherwise null is
    retruned.

    :param variable: value to calculate the sha256 hash of.
    :return: sha256 hash of the provided variable.
    """
    if not pd.isnull(variable):
        return hashlib.sha256(str(variable).encode()).hexdigest()
