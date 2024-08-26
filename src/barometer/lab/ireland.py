import logging

import numpy as np
import pandas as pd
from pandas import DataFrame

from barometer.lab._common import hashing_function

logger = logging.getLogger(__name__)


def get_result(row):
    if row["diagnostic_test"] == "PCR":
        if row["RESULT"] in [
            "Positive",
            "Weak Positive",
            "Mycoplasma bovis PCR Positive",
            "Strong Positive",
        ]:
            return 1
        elif row["RESULT"] in [
            "No Pathogen detected",
            "Negative",
            "Sterile",
            "No Significant Growth",
            "No CT",
            "Mycoplasma bovis PCR Negative",
            "Mixed Non-Significant Bacterial Growth",
            "No Significant Growth @48hrs",
            "No Growth",
            "No Pathogen detectedn",
            "No RNA detected",
            "No DNA detected",
            "No Virus Detected",
            "Not Detected",
        ]:
            return 0
        elif row["RESULT"] in [
            "Inconclusive",
            "Mixed Bacterial Growth",
            "Mixed Growth",
            "Very Mixed Growth",
        ]:
            return np.nan
    elif row["diagnostic_test"] == "Culture" and row["pathogen"] in [
        "MH",
        "PM",
        "HS",
    ]:
        if (
                row["pathogen"] == "MH"
                and row["RESULT"] == "Mannheimia haemolytica"
        ):
            return 1
        elif row["pathogen"] == "PM" and row["RESULT"] in [
            "Pasteurella multocida",
            "P. multocida",
        ]:
            return 1
        elif row["pathogen"] == "HS" and row["RESULT"] in [
            "Histophilus somni",
            "Histophilus somnus",
            "Histophilus somnii",
        ]:
            return 1
        else:
            return 0
    return np.nan


def preprocess(dataframe_raw: DataFrame) -> DataFrame:
    logger.info("Preprocessing Ireland file")
    logger.debug("Size of raw dataframe: %s rows", dataframe_raw.shape[0])

    dataframe_filtered: DataFrame = dataframe_raw.loc[
        (
            dataframe_raw["SYSTEM"]
            .str.strip()
            .isin(
                [
                    "Respiratory",
                ]
            )
        )  # np.nan
        & (
            dataframe_raw["ALIQUOTMATRIXTYPE"].isin(
                [
                    "Pleural Fluid",
                    "Tissue swab",
                    "Tonsil",
                    "Lymph Node - Multiple",
                    "Trachea",
                    "Thoracic Fluid",
                    "Lung",
                    "Swab",
                    "Culture",
                    "Thymus",
                    "Part Carcass",
                    "Tissue swab",
                    "Nasal Swab",
                    "Nasal Fluid",
                    "Tissue-Pool",
                    "Tissue (VTM)",
                    "Carcass",
                    "Lymph Node",
                    "Pooled swab",
                    "Misc.",
                ]
            )
        )
        & (
            dataframe_raw["TEST"].isin(
                [
                    "PI3V PCR",
                    "PCR M. haemolytica - ARVL",
                    "Mycoplasma bovis (PCR)",
                    "PCR H. somni - ARVL",
                    "PCR P. multocida - ARVL",
                    "Miscellaneous Test",
                    "Routine Culture",
                    "PCR M. bovis - ARVL",
                    "BRSV PCR",
                    "Culture Growth",
                    "PCR BoCoV",
                    "Mycoplasma bovis (PCR)",
                ]
            )
        )
        ].copy()

    dataframe_filtered.rename(
        columns={
            "SDGa": "file_number",
            "SAMPLEa": "sample_number",
            "HERD_NOa": "farm_id",
            "DELIVERY_DATE": "date",
            "Herd.Type": "breed",
            "County": "province",
        },
        inplace=True,
    )

    dataframe_filtered["country"] = "Ireland"
    dataframe_filtered["lab_reference"] = "5"
    dataframe_filtered["sample_type"] = np.select(
        [
            dataframe_filtered["SUBCLASS"] == "Carcass",
            dataframe_filtered["ALIQUOTMATRIXTYPE"].isin(
                [
                    "Carcass",
                    "Lung",
                    "Thymus",
                    "Lymph Node - Multiple",
                    "Tissue-Pool",
                    "Lymph Node",
                    "Tissue (VTM)",
                    "Part Carcass",
                ]
            ),
            dataframe_filtered["ALIQUOTMATRIXTYPE"].isin(
                ["Swab", "Nasal Swab", "Pooled swab", "Nasal Fluid"]
            ),
            dataframe_filtered["ALIQUOTMATRIXTYPE"].isin(
                [
                    "Trachea",
                    "Thoracic Fluid",
                    "Culture",
                    "Fluid",
                    "Misc.",
                    "Pleural Fluid",
                ]
            ),
        ],
        ["Autopsy", "Autopsy", "Swab", "Unknown"],
        default="Missing",
    )
    dataframe_filtered["diagnostic_test"] = (
        dataframe_filtered["TEST"]
        .map(
            {
                "PI3V PCR": "PCR",
                "PCR M. haemolytica - ARVL": "PCR",
                "Mycoplasma bovis (PCR)": "PCR",
                "PCR H. somni - ARVL": "PCR",
                "PCR M. bovis - ARVL": "PCR",
                "BRSV PCR": "PCR",
                "PCR BoCoV": "PCR",
                "PCR P. multocida - ARVL": "PCR",
                "Routine Culture": "Culture",
                "Culture Growth": "Culture",
            }
        )
        .fillna("Missing")
    )
    dataframe_filtered["breed"] = (
        dataframe_filtered["breed"]
        .map(
            {
                "BEEF": "Beef",
                "DAIRY": "Dairy",
                "SUCKLER": "Suckler",
                "OTHER": "Unknown",
            }
        )
        .fillna("Unknown")
    )
    dataframe_filtered["pathogen"] = (
        dataframe_filtered["TEST"]
        .map(
            {
                "PCR P. multocida - ARVL": "PM",
                "PCR M. haemolytica - ARVL": "MH",
                "PCR H. somni - ARVL": "HS",
                "H. somni PCR": "HS",
                "Mycoplasma bovis (PCR)": "MB",
                "PCR M. bovis - ARVL": "MB",
                "PI3V PCR": "PI3",
                "PCR BoCoV": "BCV",
                "BRSV PCR": "BRSV",
            }
        )
        .fillna("Missing")
    )

    barometer_dt = dataframe_filtered[
        [
            "file_number",
            "sample_number",
            "diagnostic_test",
            "country",
            "lab_reference",
            "sample_type",
            "breed",
            "pathogen",
            "date",
            "province",
            "RESULT",
            "RESULTNAME",
            "AGENT",
            "farm_id",
        ]
    ].copy()

    barometer_dt.drop_duplicates(inplace=True)
    barometer_dt.loc[:, "file_number"] = barometer_dt["file_number"].apply(
        hashing_function
    )
    barometer_dt.loc[:, "sample_number"] = barometer_dt["sample_number"].apply(
        hashing_function
    )
    barometer_dt.loc[:, "farm_id"] = barometer_dt["farm_id"].apply(
        hashing_function
    )

    barometer_dt["HS"] = np.where(
        barometer_dt["diagnostic_test"] == "Culture", 0, np.nan
    )
    barometer_dt["MH"] = np.where(
        barometer_dt["diagnostic_test"] == "Culture", 0, np.nan
    )
    barometer_dt["PM"] = np.where(
        barometer_dt["diagnostic_test"] == "Culture", 0, np.nan
    )
    barometer_dt = barometer_dt.astype(
        {
            "HS": "Int8",
            "MH": "Int8",
            "PM": "Int8",
        }
    )

    barometer_long = pd.melt(
        barometer_dt,
        id_vars=[
            "file_number",
            "sample_number",
            "diagnostic_test",
            "country",
            "lab_reference",
            "sample_type",
            "breed",
            "pathogen",
            "date",
            "province",
            "RESULT",
            "RESULTNAME",
            "AGENT",
            "farm_id",
        ],
        value_vars=["PM", "MH", "HS"],
        var_name="pathogen_culture",
        value_name="result_culture",
    )

    barometer_long.loc[barometer_long["pathogen"] == "Missing", "pathogen"] = (
        barometer_long["pathogen_culture"]
    )

    barometer_long["result"] = barometer_long.apply(get_result, axis=1)

    barometer_results = barometer_long.astype({"result": "Int8"})[
        [
            "file_number",
            "sample_number",
            "diagnostic_test",
            "country",
            "lab_reference",
            "sample_type",
            "breed",
            "pathogen",
            "result",
            "date",
            "province",
            "RESULT",
            "RESULTNAME",
            "AGENT",
            "farm_id",
        ]
    ]
    barometer_results.drop_duplicates(inplace=True)

    barometer_results["floored_date"] = (
        barometer_results["date"].dt.to_period("M").dt.to_timestamp()
    )

    barometer_grouped = (
        barometer_results.groupby(
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
            ],
            observed=True,
            dropna=False,
        )["result"]
        .max()
        .reset_index()
    )

    logger.debug(
        "Size of preprocessed dataframe: %s rows", barometer_grouped.shape[0]
    )
    logger.info("Done preprocessing GD file")
    return barometer_grouped
