import hashlib

import numpy as np
import pandas as pd
import rdflib
from rdflib import Literal
from rdflib import Namespace
from rdflib.namespace import XSD


def process_file(files) -> str:
    # Load the data into Pandas dataframes
    dfs = []
    for file in files:
        df = pd.read_excel(file, engine="openpyxl")
        dfs.append(df)

    barometer_dt_raw_2021 = dfs[0]
    barometer_dt_raw_2022 = dfs[1]

    # Combine the datasets using pd.concat
    barometer_dt_combined = pd.concat(
        [barometer_dt_raw_2021, barometer_dt_raw_2022], ignore_index=True
    )

    # Filter data using pandas
    conditions_system = barometer_dt_combined["SYSTEM"].isin(
        ["Respiratory", "NA"]
    )
    barometer_dt_filter = barometer_dt_combined[conditions_system]

    conditions_aliquot_matrix = barometer_dt_filter["ALIQUOTMATRIXTYPE"].isin(
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
    barometer_dt_filter2 = barometer_dt_filter[conditions_aliquot_matrix]

    conditions_test = barometer_dt_filter2["TEST"].isin(
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
    barometer_dt_filter3 = barometer_dt_filter2[conditions_test]

    # Data manipulation
    barometer_dt = (
        barometer_dt_filter3.rename(
            columns={
                "SDGa": "FileNumber",
                "SAMPLEa": "SampleNumber",
                "HERD_NOa": "FarmID",
                "DELIVERY_DATE": "Date",
                "Herd.Type": "Breed",
            }
        )
        .assign(
            Country="Ireland",
            LabReference="5",
            SampleType=lambda x: x["ALIQUOTMATRIXTYPE"].map(
                {
                    "Carcass": "Autopsy",
                    "Lung": "Autopsy",
                    "Thymus": "Autopsy",
                    "Lymph Node - Multiple": "Autopsy",
                    "Tissue-Pool": "Autopsy",
                    "Lymph Node": "Autopsy",
                    "Tissue (VTM)": "Autopsy",
                    "Part Carcass": "Autopsy",
                    "Swab": "Swab",
                    "Nasal Swab": "Swab",
                    "Pooled swab": "Swab",
                    "Nasal Fluid": "Swab",
                    "Trachea": "Unknown",
                    "Thoracic Fluid": "Unknown",
                    "Culture": "Unknown",
                    "Fluid": "Unknown",
                    "Misc.": "Unknown",
                    "Pleural Fluid": "Unknown",
                }
            ),
            DiagnosticTest=lambda x: x["TEST"].map(
                {
                    "PI3V PCR": "PCR",
                    "PCR M. haemolytica - ARVL": "PCR",
                    "Mycoplasma bovis (PCR)": "PCR",
                    "PCR H. somni - ARVL": "PCR",
                    "PCR M. bovis - ARVL": "PCR",
                    "BRSV PCR": "PCR",
                    "PCR BoCoV": "PCR",
                    "Mycoplasma bovis (PCR)": "PCR",
                    "PCR P. multocida - ARVL": "PCR",
                    "Routine Culture": "Culture",
                    "Culture Growth": "Culture",
                }
            ),
            Breed=lambda x: x["Breed"].map(
                {
                    "BEEF": "Beef",
                    "DAIRY": "Dairy",
                    "SUCKLER": "Suckler",
                    "OTHER": "Unknown",
                }
            ),
            Province=lambda x: x["County"],
            Pathogen=lambda x: x["TEST"].map(
                {
                    "PCR P. multocida - ARVL": "PM",
                    "PCR M. haemolytica - ARVL": "MH",
                    "PCR H. somni - ARVL": "HS",
                    "Mycoplasma bovis (PCR)": "MB",
                    "PCR M. bovis - ARVL": "MB",
                    "PI3V PCR": "PI3",
                    "PCR BoCoV": "BCV",
                    "BRSV PCR": "BRSV",
                }
            ),
        )
        .filter(
            items=[
                "FileNumber",
                "SampleNumber",
                "DiagnosticTest",
                "Country",
                "LabReference",
                "SampleType",
                "Breed",
                "Pathogen",
                "Date",
                "Province",
                "RESULT",
                "RESULTNAME",
                "AGENT",
                "FarmID",
            ]
        )
        .drop_duplicates()
        .assign(
            FileNumber=lambda x: x["FileNumber"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
            SampleNumber=lambda x: x["SampleNumber"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
            FarmID=lambda x: x["FarmID"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
        )
    )

    # Add extra rows for cultuur (& MALDI & NGS?)
    pathogens = ["HS", "MH", "PM"]
    barometer_dt["HS"] = barometer_dt.apply(
        lambda x: 0 if x["DiagnosticTest"] == "Culture" else None, axis=1
    )
    barometer_dt["MH"] = barometer_dt.apply(
        lambda x: 0 if x["DiagnosticTest"] == "Culture" else None, axis=1
    )
    barometer_dt["PM"] = barometer_dt.apply(
        lambda x: 0 if x["DiagnosticTest"] == "Culture" else None, axis=1
    )

    barometer_dt_culture_wide = pd.melt(
        barometer_dt,
        id_vars=[
            "FileNumber",
            "SampleNumber",
            "DiagnosticTest",
            "Country",
            "LabReference",
            "SampleType",
            "Breed",
            "Pathogen",
            "Date",
            "Province",
            "RESULT",
            "RESULTNAME",
            "AGENT",
            "FarmID",
        ],
        value_vars=pathogens,
        var_name="Pathogen_culture",
        value_name="Result_culture",
    )

    barometer_dt_culture_wide["Pathogen"] = barometer_dt_culture_wide.apply(
        lambda x: (
            x["Pathogen_culture"]
            if x["Pathogen"] == "Missing"
            else x["Pathogen"]
        ),
        axis=1,
    )

    # Create binary results PCR & culture
    def calculate_result(row):
        if row["DiagnosticTest"] == "PCR":
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
            else:
                return None
        elif row["DiagnosticTest"] == "Culture":
            if row["Pathogen"] in ["MH", "PM", "HS"]:
                if (
                    (
                        row["Pathogen"] == "MH"
                        and row["RESULT"] == "Mannheimia haemolytica"
                    )
                    or (
                        row["Pathogen"] == "PM"
                        and row["RESULT"]
                        in ["Pasteurella multocida", "P. multocida"]
                    )
                    or (
                        row["Pathogen"] == "HS"
                        and row["RESULT"]
                        in [
                            "Histophilus somni",
                            "Histophilus somnus",
                            "Histophilus somnii",
                        ]
                    )
                ):
                    return 1
            else:
                return 0
        return None

    barometer_results = (
        barometer_dt_culture_wide.assign(
            Result=lambda x: x.apply(calculate_result, axis=1)
        )
        .filter(
            items=[
                "FileNumber",
                "SampleNumber",
                "DiagnosticTest",
                "Country",
                "LabReference",
                "SampleType",
                "Breed",
                "Pathogen",
                "Result",
                "Date",
                "Province",
                "RESULT",
                "RESULTNAME",
                "AGENT",
                "FarmID",
            ]
        )
        .drop_duplicates()
        .assign(
            Filenumber=lambda x: x["FileNumber"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
            Samplenumber=lambda x: x["SampleNumber"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
            Farm_ID=lambda x: x["FarmID"].apply(
                lambda val: hashlib.sha256(str(val).encode()).hexdigest()
            ),
        )
    )

    barometer_results["Floored_date"] = (
        pd.to_datetime(barometer_results["Date"])
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    barometer_results["Floored_date"] = barometer_results[
        "Floored_date"
    ].dt.date

    # barometer_groupby = barometer_results.groupby(['LabReference', 'Country', 'Breed', 'Floored_date', 'Province',
    #                                            'FarmID', 'DiagnosticTest', 'SampleType', 'Pathogen']) \
    # .apply(lambda group: group.max(numeric_only=True, skipna=True) if not group[["Result"]].isna().all().all() else pd.DataFrame({"Result": [None]}))

    # barometer_groupby.reset_index(inplace=True)

    barometer_groupby = barometer_results.groupby(
        [
            "LabReference",
            "Country",
            "Breed",
            "Floored_date",
            "Province",
            "FarmID",
            "DiagnosticTest",
            "SampleType",
            "Pathogen",
        ],
        observed=True
    ).agg(
        Result=(
            "Result",
            lambda x: np.nan if all(pd.isna(x)) else max(x.dropna()),
        )
    )
    barometer_groupby.reset_index(inplace=True)

    g = rdflib.Graph()
    onto = Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    g.bind("onto", onto)
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    g.bind("xsd", xsd)

    # iterate over each row in the dataframe and
    for index, row in barometer_groupby.iterrows():
        # Create a URI for the CattleSample based on the index
        CattleSample = onto[f"CattleSample_{index}"]

        # Add triples for each attribute in the row
        g.add(
            (
                CattleSample,
                onto.hasDiagnosticTest,
                Literal(row["DiagnosticTest"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasCountry,
                Literal(row["Country"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasBreed,
                Literal(row["Breed"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasDate,
                Literal(row["Floored_date"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasProvince,
                Literal(row["Province"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasFarmIdentification,
                Literal(row["FarmID"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasSampleType,
                Literal(row["SampleType"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasPathogen,
                Literal(row["Pathogen"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasResult,
                Literal(row["Result"], datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasLabreference,
                Literal(row["LabReference"], datatype=XSD.string),
            )
        )

    filename_output = "RDFoutputCattleSampleLab5.ttl"
    g.serialize(destination=filename_output, format="turtle")
    return filename_output
