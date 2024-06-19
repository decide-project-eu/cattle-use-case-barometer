import hashlib

import numpy as np
import pandas as pd
import rdflib
from pandas import DataFrame
from rdflib import Literal
from rdflib import Namespace
from rdflib.namespace import XSD


def preprocess(dataframe_raw: DataFrame) -> DataFrame:
    """Preprocess data into report-ready format.

    The data provided was in an awkward format and wasn't suitable for
    reporting-purposes. This function makes data anonym, molds existing
    columns into multiple usable columns and shapes the data in a report-ready
    format.

    :param dataframe_raw: original, unprocessed data.
    :return: preprocessed data, ready for reporting.
    """
    # Rename columns
    dataframe_raw.rename(
        columns={
            "N° échantillon": "Dossier",
            "Date of Sample": "Date",
            "Sample Type": "SampleType",
            "METH": "DiagnosticTest",
            "TRP": "FarmID",
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
    dataframe_raw[["Postal_code", "City"]] = dataframe_raw[
        "ADDRESS"
    ].str.split(n=1, expand=True)
    # Convert Postal_code to numeric
    dataframe_raw["Postal_code"] = pd.to_numeric(
        dataframe_raw["Postal_code"], errors="coerce"
    )
    # Create new columns
    dataframe_raw["FileNumber"] = dataframe_raw["Dossier"].str.slice(
        stop=12
    )
    dataframe_raw["SampleNumber"] = dataframe_raw["Dossier"].str.slice(
        start=-3
    )
    dataframe_raw["Country"] = "Belgium"
    dataframe_raw["LabReference"] = "3"

    # Map Sample_type
    sample_type_mapping = {"BAL": "BAL", "SWAB": "Swab", "CARCASS": "Autopsy"}
    dataframe_raw["SampleType"] = dataframe_raw["SampleType"].map(
        sample_type_mapping
    )

    # Map Breed
    breed_mapping = {"MEAT": "Beef", "MILK": "Dairy", "MXD": "Mixed"}
    dataframe_raw["Breed"] = (
        dataframe_raw["SPECUL"].map(breed_mapping).fillna("Unknown")
    )

    # Map Province based on Postal_code
    postal_code_conditions = [
        (dataframe_raw["Postal_code"].between(1000, 1299)),
        (dataframe_raw["Postal_code"].between(1300, 1499)),
        (dataframe_raw["Postal_code"].between(1500, 1999)),
        (dataframe_raw["Postal_code"].between(3000, 3499)),
        (dataframe_raw["Postal_code"].between(2000, 2999)),
        (dataframe_raw["Postal_code"].between(3500, 3999)),
        (dataframe_raw["Postal_code"].between(4000, 4999)),
        (dataframe_raw["Postal_code"].between(5000, 5999)),
        (dataframe_raw["Postal_code"].between(6000, 6599)),
        (dataframe_raw["Postal_code"].between(7000, 7999)),
        (dataframe_raw["Postal_code"].between(6600, 6999)),
        (dataframe_raw["Postal_code"].between(8000, 8999)),
    ]
    province_choices = [
        "Brussels",
        "Walloon Brabant",
        "Flemish Brabant",
        "Antwerp",
        "Limburg",
        "Limburg",
        "Liège",
        "Namur",
        "Hainaut",
        "Hainaut",
        "Luxembourg",
        "West Flanders",
    ]
    dataframe_raw["Province"] = pd.Series(
        pd.Categorical(
            np.select(
                postal_code_conditions,
                province_choices,
                default="East Flanders",
            )
        )
    )

    # Select columns
    dataframe = dataframe_raw[
        [
            "FileNumber",
            "DiagnosticTest",
            "SampleNumber",
            "Country",
            "LabReference",
            "SampleType",
            "Breed",
            "PM",
            "MH",
            "HS",
            "MB",
            "BRSV",
            "PI3",
            "BCV",
            "Date",
            "Postal_code",
            "Province",
            "FarmID",
        ]
    ].copy()

    # Drop duplicates
    dataframe.drop_duplicates(inplace=True)

    # Hash Filenumber, Samplenumber, and Farm_ID
    dataframe["FileNumber"] = dataframe["FileNumber"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    dataframe["SampleNumber"] = dataframe["SampleNumber"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    dataframe["FarmID"] = dataframe["FarmID"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )

    # Add a floored_date column
    dataframe["Floored_date"] = dataframe["Date"].apply(
        lambda x: x - pd.to_timedelta(x.day - 1, unit="d")
    )

    # Group and aggregate data
    columns_to_aggregate = ["PM", "MH", "HS", "MB", "BRSV", "PI3", "BCV"]
    barometer_grouped = (
        dataframe.groupby(
            [
                "LabReference",
                "Country",
                "Breed",
                "Floored_date",
                "Province",
                "FarmID",
                "DiagnosticTest",
                "SampleType",
            ],
            observed=True,
        )[columns_to_aggregate]
        .max()
        .reset_index()
    )

    # Convert to long format
    pathogens = ['PM', 'MH', 'HS', 'MB', 'BRSV', 'PI3', 'BCV']
    barometer_long = pd.melt(
        barometer_grouped,
        value_vars=pathogens,
        var_name='Pathogen',
        value_name='Result'
    )

    return barometer_long


def process_files(files) -> str:
    # Load the data into Pandas dataframes
    dfs = []
    for file in files:
        df = pd.read_excel(file, engine="openpyxl")
        dfs.append(df)

    barometer_dt_raw = dfs[0]

    # Floor date to 1st of month
    # barometer_dt['Floored_date'] = pd.to_datetime(barometer_dt['Date']).dt.to_period('M').dt.to_timestamp()

    # Convert to long format
    # pathogens = ['PM', 'MH', 'HS', 'MB', 'BRSV', 'PI3', 'BCV']
    # barometer_long = pd.melt(barometer_groupby, id_vars=['Lab_reference', 'Country', 'Breed', 'Floored_date', 'Province', 'Farm_ID', 'Diagnostic_test', 'Sample_type'], value_vars=pathogens, var_name='Pathogen', value_name='Result')

    # Save to CSV
    # barometer_long.to_csv("../Data/CleanedData/barometer_ARSIA.csv", index=False)

    # Aggregate data based on farm_ID & month
    barometer_groupby = (
        barometer_dt.groupby(
            [
                "LabReference",
                "Country",
                "Breed",
                "Floored_date",
                "Province",
                "FarmID",
                "DiagnosticTest",
                "SampleType",
            ]
        )
        .agg(
            {
                "PM": "max",
                "MH": "max",
                "HS": "max",
                "MB": "max",
                "BRSV": "max",
                "PI3": "max",
                "BCV": "max",
            }
        )
        .reset_index()
    )

    # Convert the data to the long format:
    barometer_long = pd.melt(
        barometer_groupby,
        id_vars=[
            "LabReference",
            "Country",
            "Breed",
            "Floored_date",
            "Province",
            "FarmID",
            "DiagnosticTest",
            "SampleType",
        ],
        var_name="Pathogen",
        value_name="Result",
    )

    # Graph creation
    g = rdflib.Graph()
    onto = Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    g.bind("onto", onto)
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    g.bind("xsd", xsd)

    # Iterate through the rows of the barometer_long dataframe and create RDF triples
    for index, row in barometer_long.iterrows():
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

    filename_output = "RDFoutputCattleSampleLab3.ttl"
    g.serialize(destination=filename_output, format="turtle")
    return filename_output
