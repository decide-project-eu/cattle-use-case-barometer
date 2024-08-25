import hashlib

import pandas as pd


def hashing_function(variable):
    if not pd.isnull(variable):
        return hashlib.sha256(str(variable).encode()).hexdigest()
