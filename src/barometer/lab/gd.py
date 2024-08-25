import logging

import numpy as np
import pandas as pd
from pandas import DataFrame

from barometer.lab._common import hashing_function

logger = logging.getLogger(__name__)


def preprocess(dataframe_raw: DataFrame) -> DataFrame:
    logger.info("Preprocessing GD file")
    logger.debug("Size of raw dataframe: %s rows", dataframe_raw.size)

    dataframe_raw.rename(
        columns={
            "Dossier_ID": "file_number",
            "sample_id": "sample_number",
            "farm_ID": "farm_id",
        },
        inplace=True,
    )
    dataframe_raw["file_number"] = dataframe_raw["file_number"].astype("str")
    dataframe_raw["sample_number"] = dataframe_raw["sample_number"].astype(
        "str"
    )
    dataframe_raw["farm_id"] = dataframe_raw["farm_id"].astype("str")

    dataframe_raw["country"] = "The Netherlands"
    dataframe_raw["lab_reference"] = "2"
    dataframe_raw["sample_type"] = np.select(
        [
            dataframe_raw["reason_of_sampling"] == "Autopsy",
            dataframe_raw["sample"] == "BAL",
            dataframe_raw["sample"] == "SWABS",
            dataframe_raw["sample"] == "OTHER",
        ],
        ["Autopsy", "BAL", "Swab", "Unknown"],
        default="Missing",
    )
    dataframe_raw["diagnostic_test"] = (
        dataframe_raw["test"]
        .map({"PCR": "PCR", "Kweek": "Culture"})
        .fillna("Missing")
    )
    dataframe_raw["breed"] = (
        dataframe_raw["breed"]
        .map(
            {
                "beef": "Beef",
                "dairy": "Dairy",
                "mixed": "Mixed",
                "veal": "Veal",
                "other": "Unknown",
                "rearing": "Unknown",
                "unknown": "Unknown",
            }
        )
        .fillna("Unknown")
    )
    dataframe_raw["province"] = (
        dataframe_raw["provincie"]
        .map(
            {
                "DR": "Drenthe",
                "FL": "Flevoland",
                "FR": "Friesland",
                "GL": "Gelderland",
                "GR": "Groningen",
                "LB": "Limburg",
                "NB": "North Brabant",
                "NH": "North Holland",
                "OV": "Overijssel",
                "UT": "Utrecht",
                "ZH": "South Holland",
                "ZL": "Zeeland",
            }
        )
        .fillna("Missing")
    )

    dataframe_raw = dataframe_raw[
        [
            "file_number",
            "diagnostic_test",
            "sample_number",
            "country",
            "lab_reference",
            "sample_type",
            "breed",
            "PM",
            "MH",
            "HS",
            "MB",
            "BRSV",
            "PI3",
            "BCV",
            "date",
            "province",
            "project",
            "farm_id",
        ]
    ].copy()

    dataframe_raw.drop_duplicates(inplace=True)

    dataframe_raw.loc[:, "file_number"] = dataframe_raw["file_number"].apply(
        hashing_function
    )
    dataframe_raw.loc[:, "sample_number"] = dataframe_raw[
        "sample_number"
    ].apply(hashing_function)
    dataframe_raw.loc[:, "farm_id"] = dataframe_raw["farm_id"].apply(
        hashing_function
    )

    dataframe_raw = dataframe_raw.loc[
        dataframe_raw["project"].isin(["monitoring", "no project"])
    ]
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

    barometer_long = barometer_long.loc[
        :,
        [
            "lab_reference",
            "country",
            "breed",
            "floored_date",
            "province",
            "farm_id",
            "diagnostic_test",
            "sample_type",
            "pathogen",
            "result",
        ],
    ]

    logger.debug(
        "Size of preprocessed dataframe: %s rows", barometer_long.size
    )
    logger.info("Done preprocessing GD file")
    return barometer_long
