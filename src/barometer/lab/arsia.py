import pandas as pd
import rdflib
import hashlib
import numpy as np
from datetime import datetime
from datetime import timedelta
from pandas.api.types import CategoricalDtype
from rdflib import Literal, Namespace, RDF, URIRef
from rdflib.namespace import FOAF, XSD
from rdflib import Graph, Namespace, RDF, RDFS, OWL
from rdflib.plugins.sparql import prepareQuery


def process_files(files) -> str:
    # Load the data into Pandas dataframes
    dfs = []
    for file in files:
        df = pd.read_excel(file, engine="openpyxl")
        dfs.append(df)

    barometer_dt_raw = dfs[0]


    # Rename columns
    barometer_dt_raw.rename(
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
    barometer_dt_raw[["Postal_code", "City"]] = barometer_dt_raw["ADDRESS"].str.split(
        n=1, expand=True
    )

    # Convert Postal_code to numeric
    barometer_dt_raw["Postal_code"] = pd.to_numeric(
        barometer_dt_raw["Postal_code"], errors="coerce"
    )

    # Create new columns
    barometer_dt_raw["FileNumber"] = barometer_dt_raw["Dossier"].str.slice(stop=12)
    barometer_dt_raw["SampleNumber"] = barometer_dt_raw["Dossier"].str.slice(start=-3)
    barometer_dt_raw["Country"] = "Belgium"
    barometer_dt_raw["LabReference"] = "3"

    # Map Sample_type
    sample_type_mapping = {"BAL": "BAL", "SWAB": "Swab", "CARCASS": "Autopsy"}
    barometer_dt_raw["SampleType"] = barometer_dt_raw["SampleType"].map(sample_type_mapping)

    # Map Breed
    breed_mapping = {"MEAT": "Beef", "MILK": "Dairy", "MXD": "Mixed"}
    barometer_dt_raw["Breed"] = (
        barometer_dt_raw["SPECUL"].map(breed_mapping).fillna("Unknown")
    )

    # Map Province based on Postal_code
    postal_code_conditions = [
        (barometer_dt_raw["Postal_code"].between(1000, 1299)),
        (barometer_dt_raw["Postal_code"].between(1300, 1499)),
        (barometer_dt_raw["Postal_code"].between(1500, 1999)),
        (barometer_dt_raw["Postal_code"].between(3000, 3499)),
        (barometer_dt_raw["Postal_code"].between(2000, 2999)),
        (barometer_dt_raw["Postal_code"].between(3500, 3999)),
        (barometer_dt_raw["Postal_code"].between(4000, 4999)),
        (barometer_dt_raw["Postal_code"].between(5000, 5999)),
        (barometer_dt_raw["Postal_code"].between(6000, 6599)),
        (barometer_dt_raw["Postal_code"].between(7000, 7999)),
        (barometer_dt_raw["Postal_code"].between(6600, 6999)),
        (barometer_dt_raw["Postal_code"].between(8000, 8999)),
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
    barometer_dt_raw["Province"] = pd.Series(
        pd.Categorical(
            np.select(postal_code_conditions, province_choices, default="East Flanders")
        )
    )

    # Select columns
    barometer_dt = barometer_dt_raw[
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
    barometer_dt.drop_duplicates(inplace=True)

    # Hash Filenumber, Samplenumber, and Farm_ID
    barometer_dt["FileNumber"] = barometer_dt["FileNumber"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    barometer_dt["SampleNumber"] = barometer_dt["SampleNumber"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    barometer_dt["FarmID"] = barometer_dt["FarmID"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )


    # Floor date to 1st of month
    # barometer_dt['Floored_date'] = pd.to_datetime(barometer_dt['Date']).dt.to_period('M').dt.to_timestamp()

    # Add a floored_date column
    barometer_dt["Floored_date"] = barometer_dt["Date"].apply(
        lambda x: x - pd.to_timedelta(x.day - 1, unit="d")
    )


    # Group and aggregate data
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
        g.add((CattleSample, onto.hasCountry, Literal(row["Country"], datatype=XSD.string)))
        g.add((CattleSample, onto.hasBreed, Literal(row["Breed"], datatype=XSD.string)))
        g.add(
            (CattleSample, onto.hasDate, Literal(row["Floored_date"], datatype=XSD.string))
        )
        g.add(
            (CattleSample, onto.hasProvince, Literal(row["Province"], datatype=XSD.string))
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
            (CattleSample, onto.hasPathogen, Literal(row["Pathogen"], datatype=XSD.string))
        )
        g.add((CattleSample, onto.hasResult, Literal(row["Result"], datatype=XSD.string)))
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
