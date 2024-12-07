import logging

import numpy as np
import pandas as pd

from barometer.lab._common import hashing_function

logger = logging.getLogger(__name__)


def preprocess(
        barometer_dt_raw: pd.DataFrame,
        barometer_aero_cult_raw: pd.DataFrame,
        barometer_myco_cult_raw: pd.DataFrame
) -> pd.DataFrame:
    """Preprocess data into report-ready format.

    The data provided was in an awkward format and wasn't suitable for
    reporting-purposes. This function makes data anonym, molds existing
    columns into multiple usable columns and shapes the data in a report-ready
    format.

    :param barometer_dt_raw: original, unprocessed data.
    :param barometer_aero_cult_raw: original, unprocessed data.
    :param barometer_myco_cult_raw: original, unprocessed data.
    :return: preprocessed data, ready for reporting.
    """
    logger.info("Preprocessing DGZ files")
    logger.debug("Size of raw barometer_dt_raw: %s rows", barometer_dt_raw.shape[0])
    logger.debug("Size of raw barometer_aero_cult_raw: %s rows", barometer_aero_cult_raw.shape[0])
    logger.debug("Size of raw barometer_myco_cult_raw: %s rows", barometer_myco_cult_raw.shape[0])

    # Preprocess barometer_aero_cult_raw
    barometer_aero_cult = (
        barometer_aero_cult_raw.rename(
            columns={
                "Dossiernummer": "Filenumber",
                "KIEMSTAAL IDENTIFICATIE": "Pathogen_identification",
                "KIEMSTAAL RESULTAAT": "Pathogen_result",
                "Staalnummer": "Samplenumber",
            }
        )
        .assign(Parameter_code="BAC_AERO", Result="OK")
        .filter(
            items=[
                "Filenumber",
                "Pathogen_identification",
                "Pathogen_result",
                "Parameter_code",
                "Samplenumber",
                "Result",
            ]
        )
    )
    barometer_aero_cult = barometer_aero_cult[
        barometer_aero_cult["Pathogen_identification"].isin(
            [
                "Pasteurella multocida",
                "Mannheimia haemolytica",
                "Histophilus somni",
                "Mycoplasma bovis",
            ]
        )
    ]
    barometer_aero_cult.drop_duplicates(inplace=True)
    barometer_aero_cult["Filenumber"] = barometer_aero_cult[
        "Filenumber"
    ].apply(hashing_function)
    barometer_aero_cult["Samplenumber"] = barometer_aero_cult[
        "Samplenumber"
    ].apply(hashing_function)

    # Preprocess barometer_myco_cult_raw
    barometer_myco_cult = (
        barometer_myco_cult_raw.rename(
            columns={
                "Dossiernummer": "Filenumber",
                "KIEMSTAAL IDENTIFICATIE": "Pathogen_identification",
                "KIEMSTAAL RESULTAAT": "Mycoplasma_result",
                "Staalnummer": "Samplenumber",
            }
        )
        .assign(Parameter_code="BAC_MYCOPLASMA", Result="OK")
        .filter(
            items=[
                "Filenumber",
                "Pathogen_identification",
                "Mycoplasma_result",
                "Parameter_code",
                "Samplenumber",
                "Result",
            ]
        )
    )
    barometer_myco_cult = barometer_myco_cult[
        barometer_myco_cult["Pathogen_identification"].isin(
            ["Mycoplasma bovis"]
        )
    ]
    barometer_myco_cult.drop_duplicates(inplace=True)
    barometer_myco_cult["Filenumber"] = barometer_myco_cult[
        "Filenumber"
    ].apply(hashing_function)
    barometer_myco_cult["Samplenumber"] = barometer_myco_cult[
        "Samplenumber"
    ].apply(hashing_function)

    # Preprocess barometer_dt_raw
    barometer_dt = (
        barometer_dt_raw.rename(
            columns={
                "Dossiernummer": "Filenumber",
                "Staalnummer": "Samplenumber",
                "Staaltype": "Sample_type",
                "PARAMETER_CODE": "Parameter_code",
                "Onderzoek": "Pathogen",
                "Resultaat": "Result",
                "Creatiedatum": "Date",
                "Postcode": "Postal_code",
                "ANON_ID": "Farm_ID",
            }
        )
        .assign(
            Country="Belgium",
            Diagnostic_test=np.where(
                barometer_dt_raw["PARAMETER_CODE"].isin(
                    ["BAC_AERO", "BAC_MYCOPLASMA"]
                ),
                "Culture",
                "PCR",
            ),
            Lab_reference="1",
        )
        .replace(
            {
                "Sample_type": [
                    "RU Broncho-alveolar lavage (BAL)",
                ]
            },
            "BAL",
        )
        .replace(
            {
                "Sample_type": [
                    "RU Anderen",
                ]
            },
            "Unknown",
        )
        .replace(
            {
                "Sample_type": [
                    "RU Swabs",
                    "RU Swab",
                    "RU Neusswab",
                    "RU Neusswabs",
                ]
            },
            "Swab",
        )
        .replace({"Sample_type": ["RU Kadaver", "RU Organen"]}, "Autopsy")
        .replace({"Sample_type": [np.nan]}, "Missing")
        .assign(
            Breed=np.where(
                barometer_dt_raw["Bedrijfstype"] == "VCALF",
                "Veal",
                np.where(
                    barometer_dt_raw["MEAT"].isnull(),
                    "Unknown",
                    np.where(
                        (barometer_dt_raw["MEAT"] / barometer_dt_raw["TOTAL"])
                        > 0.9,
                        "Beef",
                        np.where(
                            (
                                barometer_dt_raw["MILK"]
                                / barometer_dt_raw["TOTAL"]
                            )
                            > 0.9,
                            "Dairy",
                            "Mixed",
                        ),
                    ),
                ),
            )
        )
        .replace(
            {
                "Pathogen": [
                    "AD Pasteurella multocida Ag (PCR)",
                    "AD Pasteurella multocida Ag pool (PCR)",
                    "AD P. multocida Ag (PCR)",
                    "AD P. multocida Ag pool (PCR)",
                ]
            },
            "Pasteurella multocida",
        )
        .replace(
            {
                "Pathogen": [
                    "AD Mannheimia haemolytica Ag (PCR)",
                    "AD Mannheimia haemolytica Ag pool (PCR)",
                ]
            },
            "Mannheimia haemolytica",
        )
        .replace(
            {"Pathogen": ["RU PI3 Ag (PCR)", "RU PI3 Ag pool (PCR)"]}, "PI3"
        )
        .replace(
            {"Pathogen": ["RU BRSV Ag (PCR)", "RU BRSV Ag pool (PCR)"]}, "BRSV"
        )
        .replace(
            {
                "Pathogen": [
                    "AD Histophilus somnus (PCR)",
                    "AD Histophilus somnus Ag (PCR)",
                    "AD Histophilus somnus Ag pool (PCR)",
                    "AD Histophilus somni Ag (PCR)",
                    "AD Histophilus somni Ag pool (PCR)",
                ]
            },
            "Histophilus somni",
        )
        .replace(
            {
                "Pathogen": [
                    "RU Mycoplasma bovis (PCR)",
                    "RU Mycoplasma bovis Ag pool (PCR)",
                    "RU Mycoplasma bovis Ag (PCR)",
                ]
            },
            "Mycoplasma bovis",
        )
        .replace(
            {"Pathogen": ["AD Corona Ag (PCR)", "AD Corona Ag pool (PCR)"]},
            "BCV",
        )
        .assign(
            Province=pd.cut(
                barometer_dt_raw["Postcode"],
                bins=[
                    999,
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
                ordered=False,
            )
            .cat.add_categories("East Flanders")
            .fillna("East Flanders")
        )
        .filter(
            items=[
                "Filenumber",
                "Diagnostic_test",
                "Samplenumber",
                "Country",
                "Lab_reference",
                "Sample_type",
                "Breed",
                "Parameter_code",
                "Result",
                "Pathogen",
                "Date",
                "Province",
                "Farm_ID",
            ]
        )
        .drop_duplicates()
    )
    barometer_dt["Filenumber"] = barometer_dt["Filenumber"].apply(
        hashing_function
    )
    barometer_dt["Samplenumber"] = barometer_dt["Samplenumber"].apply(
        hashing_function
    )
    barometer_dt["Farm_ID"] = barometer_dt["Farm_ID"].apply(hashing_function)

    df_samples = pd.DataFrame(
        [
            ("OK", "BAC_AERO", "Culture", "Pasteurella multocida"),
            ("OK", "BAC_AERO", "Culture", "Mannheimia haemolytica"),
            ("OK", "BAC_AERO", "Culture", "Histophilus somni"),
            ("OK", "BAC_MYCOPLASMA", "Culture", "Mycoplasma bovis"),
        ],
        columns=[
            "Result",
            "Parameter_code",
            "Diagnostic_test",
            "Pathogen_identification",
        ],
    )

    barometer = (
        barometer_dt.merge(
            df_samples,
            on=["Diagnostic_test", "Result", "Parameter_code"],
            how="left",
        )
        .merge(
            barometer_aero_cult,
            on=[
                "Filenumber",
                "Samplenumber",
                "Result",
                "Parameter_code",
                "Pathogen_identification",
            ],
            how="left",
        )
        .merge(
            barometer_myco_cult,
            on=[
                "Filenumber",
                "Samplenumber",
                "Result",
                "Parameter_code",
                "Pathogen_identification",
            ],
            how="left",
        )
    )
    barometer["Floored_date"] = (
        barometer["Date"].dt.to_period("M").dt.to_timestamp()
    )
    pathogen_map = {
        "Pasteurella multocida": "PM",
        "Histophilus somni": "HS",
        "Mannheimia haemolytica": "MH",
        "Mycoplasma bovis": "MB",
    }
    barometer["Pathogen"] = (
        barometer["Pathogen"].map(pathogen_map).fillna(barometer["Pathogen"])
    )
    barometer["Pathogen"] = (
        barometer["Pathogen_identification"]
        .map(pathogen_map)
        .fillna(barometer["Pathogen"])
    )
    barometer["Result"] = barometer["Result"].map(
        {
            "Twijfelachtig (PCR)": 1,
            "POSITIEF": 1,
            "GEDETECTEERD": 1,
            "GEDETECTEERD (sterk)": 1,
            "GEDETECTEERD (zwak)": 1,
            "GEDETECTEERD (matig)": 1,
            "GEDETECTEERD (zeer sterk)": 1,
            "GEDETECTEERD (zeer zwak)": 1,
            "negatief": 0,
            "Niet gedetecteerd": 0,
            "NI": np.nan,
            "niet interpreteerbaar": np.nan,
            "Inhibitie": np.nan,
        }
    )
    barometer.loc[
        (barometer["Parameter_code"] == "BAC_AERO")
        & (barometer["Pathogen_result"].isna()),
        "Result",
    ] = 0
    barometer.loc[
        (barometer["Parameter_code"] == "BAC_AERO")
        & (~barometer["Pathogen_result"].isna()),
        "Result",
    ] = 1
    barometer.loc[
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"].isna()),
        "Result",
    ] = np.nan
    barometer.loc[
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"] == "neg"),
        "Result",
    ] = 0
    barometer.loc[
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"].str.contains("POS")),
        "Result",
    ] = 1

    barometer = (
        barometer.groupby(
            [
                "Lab_reference",
                "Country",
                "Breed",
                "Floored_date",
                "Province",
                "Farm_ID",
                "Diagnostic_test",
                "Sample_type",
                "Pathogen",
            ],
            observed=True,
        )["Result"]
        .max()
        .reset_index()
        .rename(
            columns={
                "Lab_reference": "lab_reference",
                "Country": "country",
                "Breed": "breed",
                "Floored_date": "floored_date",
                "Province": "province",
                "Farm_ID": "farm_id",
                "Diagnostic_test": "diagnostic_test",
                "Sample_type": "sample_type",
                "Pathogen": "pathogen",
                "Result": "result"
            }
        )
    )

    logger.debug(
        "Size of preprocessed dataframe: %s rows", barometer.shape[0]
    )
    logger.info("Done preprocessing DGZ files")
    return barometer
