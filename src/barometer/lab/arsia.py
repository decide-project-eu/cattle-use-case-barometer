import logging

import pandas as pd
from pandas import DataFrame, melt

from barometer.lab._common import hashing_function

logger = logging.getLogger(__name__)


def preprocess(dataframe_raw: DataFrame) -> DataFrame:
    """Preprocess data into report-ready format.

    The data provided was in an awkward format and wasn't suitable for
    reporting-purposes. This function makes data anonym, molds existing
    columns into multiple usable columns and shapes the data in a report-ready
    format.

    :param dataframe_raw: original, unprocessed data.
    :return: preprocessed data, ready for reporting.
    """
    logger.info("Preprocessing ARSIA file")
    logger.debug("Size of raw dataframe: %s rows", dataframe_raw.size)

    # Rename columns
    dataframe_raw.rename(
        columns={
            "N° échantillon": "dossier",
            "Date of Sample": "date",
            "Sample Type": "sample_type",
            "METH": "diagnostic_test",
            "TRP": "farm_id",
            "P_multocida": "PM",
            "M_haemolytica": "MH",
            "H_somnus": "HS",
            "M_bovis": "MB",
            "BRSV": "BRSV",
            "PI3": "PI3",
            "Coronavirus": "BCV",
        },
        inplace=True,
    )

    # Separate ADDRESS column into Postal_code and City
    # dataframe_raw[["Postal_code", "City"]] = dataframe_raw[
    #     "ADDRESS"
    # ].str.split(n=1, expand=True)
    dataframe_raw["postal_code"] = (
        dataframe_raw["ADDRESS"].str.extract(r"(\d+)").astype("Int16")
    )

    # Create new columns
    dataframe_raw["file_number"] = dataframe_raw["dossier"].str.slice(stop=12)
    dataframe_raw["sample_number"] = dataframe_raw["dossier"].str.slice(
        start=-3
    )
    dataframe_raw["country"] = "Belgium"
    dataframe_raw["lab_reference"] = "3"

    # Map Sample_type
    sample_type_mapping = {"BAL": "BAL", "SWAB": "Swab", "CARCASS": "Autopsy"}
    dataframe_raw["sample_type"] = dataframe_raw["sample_type"].map(
        sample_type_mapping
    )

    # Map Breed
    breed_mapping = {"MEAT": "Beef", "MILK": "Dairy", "MXD": "Mixed"}
    dataframe_raw["breed"] = (
        dataframe_raw["SPECUL"].map(breed_mapping).fillna("Unknown")
    )
    dataframe_raw["province"] = (
        pd.cut(
            dataframe_raw["postal_code"],
            bins=[
                1000,
                1299,
                1499,
                1999,
                2999,
                3499,
                3999,
                4999,
                5999,
                6599,
                6999,
                7999,
                8999,
            ],
            labels=[
                "Brussels",
                "Walloon Brabant",
                "Flemish Brabant",
                "Limburg",
                "Antwerp",
                "Limburg",
                "Liège",
                "Namur",
                "Hainaut",
                "Luxembourg",
                "Hainaut",
                "West Flanders",
            ],
            include_lowest=True,
            ordered=False,
        )
        .cat.add_categories("East Flanders")
        .fillna("East Flanders")
    )

    # Select columns
    dataframe = dataframe_raw[
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
            "postal_code",
            "province",
            "farm_id",
        ]
    ].copy()

    # Drop duplicates
    dataframe.drop_duplicates(inplace=True)

    # Hash Filenumber, Samplenumber, and Farm_ID
    dataframe["file_number"] = dataframe["file_number"].apply(hashing_function)
    dataframe["sample_number"] = dataframe["sample_number"].apply(
        hashing_function
    )
    dataframe["farm_id"] = dataframe["farm_id"].apply(hashing_function)

    # Add a floored_date column
    dataframe["floored_date"] = (
        dataframe["date"].dt.to_period("M").dt.to_timestamp()
    )

    # Group and aggregate data
    columns_to_aggregate = ["PM", "MH", "HS", "MB", "BRSV", "PI3", "BCV"]
    barometer_grouped = (
        dataframe.groupby(
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

    # Convert to long format
    barometer_long = melt(
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
    logger.info("Done preprocessing ARSIA file")
    return barometer_long
