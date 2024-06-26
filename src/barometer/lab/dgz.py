import hashlib

import numpy as np
import pandas as pd
import rdflib
from rdflib import Literal
from rdflib import Namespace, RDF
from rdflib.namespace import XSD


def process_file(files) -> str:
    # Define the file paths

    # Load the data into Pandas dataframes
    dfs = []
    for file in files:
        df = pd.read_excel(file, engine="openpyxl")
        dfs.append(df)

    barometer_dt_raw = dfs[0]
    barometer_aero_cult_raw = dfs[1]
    barometer_myco_cult_raw = dfs[2]

    # Data manipulation AEROBIC CULTURE results
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
        .query(
            'Pathogen_identification in ["Pasteurella multocida", "Mannheimia haemolytica", "Histophilus somni", "Mycoplasma bovis"]'
        )
        .drop_duplicates()
    )

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

    # Data manipulation MYCOPLASMA CULTURE results
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
        .loc[
            barometer_myco_cult_raw["KIEMSTAAL IDENTIFICATIE"]
            == "Mycoplasma bovis"
        ]
        .drop_duplicates(
            subset=[
                "Filenumber",
                "Pathogen_identification",
                "Mycoplasma_result",
                "Parameter_code",
                "Samplenumber",
                "Result",
            ]
        )[
            [
                "Filenumber",
                "Pathogen_identification",
                "Mycoplasma_result",
                "Parameter_code",
                "Samplenumber",
                "Result",
            ]
        ]
    )

    # print(barometer_myco_cult)

    # Data manipulation PCR results
    barometer_dtt = (
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
            Country=np.where(
                barometer_dt_raw["PARAMETER_CODE"].isin(
                    ["BAC_AERO", "BAC_MYCOPLASMA"]
                ),
                "Belgium",
                np.nan,
            )
        )
        .assign(
            Diagnostic_test=np.where(
                barometer_dt_raw["PARAMETER_CODE"].isin(
                    ["BAC_AERO", "BAC_MYCOPLASMA"]
                ),
                "Culture",
                "PCR",
            )
        )
        .assign(Lab_reference="1")
        .replace(
            {
                "RU Broncho-alveolar lavage (BAL)": "BAL",
                "RU Anderen": "Unknown",
                "RU Swabs": "Swab",
                "RU Swab": "Swab",
                "RU Neusswab": "Swab",
                "RU Neusswabs": "Swab",
                "RU Kadaver": "Autopsy",
                "RU Organen": "Autopsy",
                np.nan: "Missing",
            }
        )
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
        )[
            [
                "Filenumber",
                "Pathogen",
                "Result",
                "Parameter_code",
                "Samplenumber",
                "Result",
                "Country",
                "Diagnostic_test",
                "Lab_reference",
                "Sample_type",
                "Postal_code",
                "Farm_ID",
                "Breed",
            ]
        ]
    )

    # Data manipulation PCR results
    barometer_dtt = barometer_dt_raw.rename(
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

    barometer_dtt["Country"] = np.where(
        barometer_dtt["Parameter_code"].isin(["BAC_AERO", "BAC_MYCOPLASMA"]),
        "Belgium",
        np.nan,
    )
    barometer_dtt["Diagnostic_test"] = np.where(
        barometer_dtt["Parameter_code"].isin(["BAC_AERO", "BAC_MYCOPLASMA"]),
        "Culture",
        "PCR",
    )
    barometer_dtt["Lab_reference"] = "1"

    sample_type_mapping = {
        "RU Broncho-alveolar lavage (BAL)": "BAL",
        "RU Anderen": "Unknown",
        "RU Swabs": "Swab",
        "RU Swab": "Swab",
        "RU Neusswab": "Swab",
        "RU Neusswabs": "Swab",
        "RU Kadaver": "Autopsy",
        "RU Organen": "Autopsy",
    }

    barometer_dtt["Sample_type"] = (
        barometer_dtt["Sample_type"].map(sample_type_mapping).fillna("Missing")
    )

    breed_mapping = {"VCALF": "Veal", "MEAT": np.nan}
    barometer_dtt["Breed"] = np.select(
        [
            (barometer_dtt["Bedrijfstype"] == "VCALF"),
            (barometer_dtt["MEAT"].isnull()),
            ((barometer_dtt["MEAT"] / barometer_dtt["TOTAL"]) > 0.9),
            ((barometer_dtt["MILK"] / barometer_dtt["TOTAL"]) > 0.9),
        ],
        ["Veal", "Unknown", "Beef", "Dairy"],
        default="Mixed",
    )

    pathogen_mapping = {
        "AD Pasteurella multocida Ag (PCR)": "Pasteurella multocida",
        "AD Pasteurella multocida Ag pool (PCR)": "Pasteurella multocida",
        "AD P. multocida Ag (PCR)": "Pasteurella multocida",
        "AD P. multocida Ag pool (PCR)": "Pasteurella multocida",
        "AD Mannheimia haemolytica Ag (PCR)": "Mannheimia haemolytica",
        "AD Mannheimia haemolytica Ag pool (PCR)": "Mannheimia haemolytica",
        "RU PI3 Ag (PCR)": "PI3",
        "RU PI3 Ag pool (PCR)": "PI3",
        "RU BRSV Ag (PCR)": "BRSV",
        "RU BRSV Ag pool (PCR)": "BRSV",
        "AD Histophilus somnus (PCR)": "Histophilus somni",
        "AD Histophilus somnus Ag (PCR)": "Histophilus somni",
        "AD Histophilus somnus Ag pool (PCR)": "Histophilus somni",
        "AD Histophilus somni Ag (PCR)": "Histophilus somni",
        "AD Histophilus somni Ag pool (PCR)": "Histophilus somni",
        "RU Mycoplasma bovis (PCR)": "Mycoplasma bovis",
        "RU Mycoplasma bovis Ag pool (PCR)": "Mycoplasma bovis",
        "RU Mycoplasma bovis Ag (PCR)": "Mycoplasma bovis",
        "AD Corona Ag (PCR)": "BCV",
        "AD Corona Ag pool (PCR)": "BCV",
    }

    # Create a new column 'Disease' based on the mapping between Pathogen and Disease
    barometer_dtt["Disease"] = barometer_dtt["Pathogen"].replace(
        pathogen_mapping
    )

    # Create a mapping between postal codes and provinces
    province_map = [
        (1000, 1299, "Brussels"),
        (1300, 1499, "Walloon Brabant"),
        (1500, 1999, "Flemish Brabant"),
        (3000, 3499, "Antwerp"),
        (2000, 2999, "Limburg"),
        (5000, 5999, "Namur"),
        (6000, 6599, "Hainaut"),
        (7000, 7999, "Hainaut"),
        (6600, 6999, "Luxembourg"),
        (8000, 8999, "West Flanders"),
    ]

    # Sort the province_map list by the first element of each tuple
    province_map.sort(key=lambda x: x[0])

    # Create a new column 'Province' based on the mapping between Postal_code and Province
    barometer_dtt["Province"] = pd.cut(
        barometer_dtt["Postal_code"],
        bins=[p[0] - 1 for p in province_map]
        + [max([p[1] for p in province_map]) + 1],
        labels=[p[2] for p in province_map],
        ordered=False,
    )

    # Select columns of interest and drop duplicates
    barometer_dtt = barometer_dtt.loc[
        :,
        [
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
            "Postal_code",
            "Province",
            "Farm_ID",
        ],
    ].drop_duplicates()

    # Show the resulting dataframe
    # print(barometer_dtt.head())

    # Join dataframes
    barometer = pd.merge(
        barometer_dtt,
        df_samples,
        on=["Diagnostic_test", "Result", "Parameter_code"],
        how="left",
    )
    barometer = pd.merge(
        barometer,
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
    barometer = pd.merge(
        barometer,
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

    # Replace values in Pathogen column
    barometer["Pathogen"] = np.where(
        barometer["Pathogen"] == "Pasteurella multocida",
        "PM",
        np.where(
            barometer["Pathogen"] == "Histophilus somni",
            "HS",
            np.where(
                barometer["Pathogen"] == "Mannheimia haemolytica",
                "MH",
                np.where(
                    barometer["Pathogen"] == "Mycoplasma bovis",
                    "MB",
                    barometer["Pathogen"],
                ),
            ),
        ),
    )

    barometer["Pathogen"] = np.where(
        barometer["Pathogen_identification"] == "Pasteurella multocida",
        "PM",
        np.where(
            barometer["Pathogen_identification"] == "Histophilus somni",
            "HS",
            np.where(
                barometer["Pathogen_identification"]
                == "Mannheimia haemolytica",
                "MH",
                np.where(
                    barometer["Pathogen_identification"] == "Mycoplasma bovis",
                    "MB",
                    barometer["Pathogen"],
                ),
            ),
        ),
    )

    # Replace values in Result column
    conditions = [
        barometer["Result"].isin(
            [
                "Twijfelachtig (PCR)",
                "POSITIEF",
                "GEDETECTEERD",
                "GEDETECTEERD (sterk)",
                "GEDETECTEERD (zwak)",
                "GEDETECTEERD (matig)",
                "GEDETECTEERD (zeer sterk)",
                "GEDETECTEERD (zeer zwak)",
            ]
        ),
        barometer["Result"].isin(["negatief", "Niet gedetecteerd"]),
        barometer["Result"].isin(["NI", "niet interpreteerbaar", "Inhibitie"]),
        (barometer["Parameter_code"] == "BAC_AERO")
        & (barometer["Pathogen_result"].isnull()),
        (barometer["Parameter_code"] == "BAC_AERO")
        & (barometer["Pathogen_result"].notnull()),
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"].isnull()),
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"] == "neg"),
        (barometer["Parameter_code"] == "BAC_MYCOPLASMA")
        & (barometer["Mycoplasma_result"].str.contains("POS")),
    ]

    choices = [1, 0, None, 0, 1, None, 0, 1]

    barometer["Result"] = np.select(conditions, choices, default=None)
    # print(barometer.head())

    g = rdflib.Graph()
    onto = Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    g.bind("onto", onto)
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    g.bind("xsd", xsd)

    # iterate over each row in the dataframe and
    for _, row in barometer.iterrows():

        # Generate anonymized values for file number and sample number
        FileNumber = hashlib.sha256(str(row.iloc[0]).encode()).hexdigest()
        SampleNumber = hashlib.sha256(str(row.iloc[2]).encode()).hexdigest()

        CattleSample = onto[f"CattleSample{row[0]}"]
        g.add((CattleSample, RDF.type, onto.CattleSample))
        # Add anonymized values to the RDF graph
        g.add(
            (
                CattleSample,
                onto.hasFileNumber,
                Literal(FileNumber, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasSampleNumber,
                Literal(SampleNumber, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasDiagnosticTest,
                Literal(row[1], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasCountry,
                Literal(row[3], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasLabReference,
                Literal(row[4], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasSampleType,
                Literal(row[5], datatype=XSD.string),
            )
        )
        g.add(
            (CattleSample, onto.hasBreed, Literal(row[6], datatype=XSD.string))
        )
        g.add(
            (
                CattleSample,
                onto.hasParameterCode,
                Literal(row[7], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasResult,
                Literal(row[8], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasPathogen,
                Literal(row[9], datatype=XSD.string),
            )
        )
        g.add(
            (CattleSample, onto.hasDate, Literal(row[10], datatype=XSD.string))
        )
        g.add(
            (
                CattleSample,
                onto.hasPostalCode,
                Literal(row[11], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasProvince,
                Literal(row[12], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasFarmID,
                Literal(row[13], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasPathogenIdentification,
                Literal(row[14], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasPathogenResult,
                Literal(row[15], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasMicoplasmaResult,
                Literal(row[16], datatype=XSD.string),
            )
        )

    # output RDF graph to file (replace with your desired filename)
    filename_output = "output/RDFOutputDGZ.ttl"
    g.serialize(destination=filename_output, format="turtle")
    return filename_output
