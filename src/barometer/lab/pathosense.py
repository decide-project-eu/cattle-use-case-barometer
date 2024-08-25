import logging

import numpy as np
import pandas as pd
from pandas import DataFrame
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import XSD

from barometer.lab._common import hashing_function

logger = logging.getLogger(__name__)


def preprocess(dataframe_raw: DataFrame) -> DataFrame:
    logger.info("Preprocessing PathoSense file")
    logger.debug("Size of raw dataframe: %s rows", dataframe_raw.size)

    dataframe_raw.rename(
        columns={
            "sample_id": "file_number",
            "farm_id": "farm_id",
            "created": "date",
        },
        inplace=True,
    )
    dataframe_raw["lab_reference"] = "4"
    dataframe_raw["diagnostic_test"] = "NPS"
    dataframe_raw["breed"] = "Unknown"
    dataframe_raw["province"] = np.nan
    dataframe_raw["country"] = dataframe_raw["country"].map(
        {"BE": "Belgium", "NL": "The Netherlands"}
    )
    dataframe_raw["sample_type"] = (
        dataframe_raw["type"]
        .map({"balFluid": "BAL", "noseSwab": "Swab"})
        .fillna("Other")
    )
    dataframe_raw["HS"] = (
        dataframe_raw["pathogens"]
        .str.contains("Histophilus somni")
        .astype("Int8")
    )
    dataframe_raw["MH"] = (
        dataframe_raw["pathogens"]
        .str.contains("Mannheimia haemolytica")
        .astype("Int8")
    )
    dataframe_raw["PM"] = (
        dataframe_raw["pathogens"]
        .str.contains("Pasteurella multocid")
        .astype("Int8")
    )
    dataframe_raw["BCV"] = (
        dataframe_raw["pathogens"]
        .str.contains("Bovine coronavirus")
        .astype("Int8")
    )
    dataframe_raw["MB"] = (
        dataframe_raw["pathogens"]
        .str.contains("Mycoplasmopsis bovis")
        .astype("Int8")
    )
    dataframe_raw["PI3"] = (
        dataframe_raw["pathogens"]
        .str.contains("Bovine respirovirus 3")
        .astype("Int8")
    )
    dataframe_raw["BRSV"] = (
        dataframe_raw["pathogens"]
        .str.contains("Bovine orthopneumovirus")
        .astype("Int8")
    )

    dataframe_raw = dataframe_raw[
        [
            "file_number",
            "lab_reference",
            "country",
            "breed",
            "province",
            "farm_id",
            "diagnostic_test",
            "sample_type",
            "PM",
            "MH",
            "HS",
            "MB",
            "BRSV",
            "PI3",
            "BCV",
            "date",
        ]
    ].copy()

    dataframe_raw.loc[:, "file_number"] = dataframe_raw["file_number"].apply(
        hashing_function
    )
    dataframe_raw.loc[:, "farm_id"] = dataframe_raw["farm_id"].apply(
        hashing_function
    )

    dataframe_raw["floored_date"] = (
        dataframe_raw["date"].dt.to_period("M").dt.to_timestamp()
    )

    # Group and aggregate data
    columns_to_aggregate = ["PM", "MH", "HS", "MB", "BRSV", "PI3", "BCV"]
    barometer_grouped = (
        dataframe_raw.groupby(
            [
                "lab_reference",
                "country",
                "breed",
                "floored_date",
                "province",
                "farm_id",
                "diagnostic_test",
                "sample_type",
            ],
            observed=True,
            dropna=False,
        )[columns_to_aggregate]
        .max()
        .reset_index()
    )

    barometer_long = pd.melt(
        barometer_grouped,
        id_vars=[
            "lab_reference",
            "country",
            "breed",
            "floored_date",
            "province",
            "farm_id",
            "diagnostic_test",
            "sample_type",
        ],
        var_name="pathogen",
        value_name="result",
    )

    logger.debug(
        "Size of preprocessed dataframe: %s rows", barometer_long.size
    )
    logger.info("Done preprocessing PathoSense file")
    return barometer_long
