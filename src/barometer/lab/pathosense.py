import pandas as pd
import rdflib
import hashlib
import numpy as np
from datetime import datetime
from pandas.api.types import CategoricalDtype
from rdflib import Literal, Namespace, RDF, URIRef
from rdflib.namespace import FOAF, XSD
from rdflib import Graph, Namespace, RDF, RDFS, OWL
from rdflib.plugins.sparql import prepareQuery


def process_file(files) -> str:
    # Load the data into Pandas dataframes
    dfs = []
    for file in files:
        df = pd.read_csv(file)
        dfs.append(df)

    barometer_dt_raw = dfs[0]

    # Rename columns
    barometer_dt_raw.rename(
        columns={
            "sample_id": "FileNumber",
            "farm_id": "FarmID",
            "created": "Date",
        },
        inplace=True,
    )

    # Mutate new columns
    barometer_dt_raw["LabReference"] = "4"
    barometer_dt_raw["DiagnosticTest"] = "NPS"
    barometer_dt_raw["Breed"] = "Unknown"
    barometer_dt_raw["Province"] = pd.NA

    # Map values for Country column
    country_mapping = {"BE": "Belgium", "NL": "The Netherlands"}
    barometer_dt_raw["Country"] = barometer_dt_raw["country"].map(
        country_mapping
    )

    # Map values for Sample_type column
    sample_type_mapping = {"balFluid": "BAL", "noseSwab": "Swab"}
    barometer_dt_raw["SampleType"] = (
        barometer_dt_raw["type"].map(sample_type_mapping).fillna("Other")
    )
    barometer_dt_raw["Province"].fillna("Unknown", inplace=True)

    # Fill missing values in pathogens column with empty string
    barometer_dt_raw["pathogens"].fillna("", inplace=True)

    # Create new columns for pathogens
    barometer_dt_raw["HS"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Histophilus somni")
        .astype(int)
    )
    barometer_dt_raw["MH"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Mannheimia haemolytica")
        .astype(int)
    )
    barometer_dt_raw["PM"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Pasteurella multocida")
        .astype(int)
    )
    barometer_dt_raw["BCV"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Bovine coronavirus")
        .astype(int)
    )
    barometer_dt_raw["MB"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Mycoplasmopsis bovis")
        .astype(int)
    )
    barometer_dt_raw["PI3"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Bovine respirovirus 3")
        .astype(int)
    )
    barometer_dt_raw["BRSV"] = (
        barometer_dt_raw["pathogens"]
        .str.contains("Bovine orthopneumovirus")
        .astype(int)
    )

    # Select desired columns
    barometer_dt = barometer_dt_raw[
        [
            "FileNumber",
            "LabReference",
            "Country",
            "Breed",
            "Province",
            "FarmID",
            "DiagnosticTest",
            "SampleType",
            "PM",
            "MH",
            "HS",
            "MB",
            "BRSV",
            "PI3",
            "BCV",
            "Date",
        ]
    ]

    # Drop duplicates
    barometer_dt.drop_duplicates(inplace=True)

    # Convert Filenumber and Farm_ID to SHA256 hash
    barometer_dt["FileNumber"] = barometer_dt["FileNumber"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    barometer_dt["FarmID"] = barometer_dt["FarmID"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )

    # Convert Date column to datetime
    barometer_dt["Date"] = pd.to_datetime(barometer_dt["Date"])

    # Floor date to 1st of month
    barometer_dt["Floored_date"] = (
        barometer_dt["Date"].dt.to_period("M").dt.to_timestamp()
    )

    # Aggregate data based on farm_ID & month
    barometer_groupby = barometer_dt.groupby(
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
    )[["PM", "MH", "HS", "MB", "BRSV", "PI3", "BCV"]].max(min_count=1)

    # Convert to long
    barometer_long = barometer_groupby.reset_index().melt(
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

    # Convert Floored_date back to datetime (for consistency)
    barometer_long["Floored_date"] = pd.to_datetime(
        barometer_long["Floored_date"]
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

    filename_output = "RDFoutputCattleSampleLab4.ttl"
    g.serialize(destination=filename_output, format="turtle")
    return filename_output
