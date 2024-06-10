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
        df = pd.read_excel(file, engine="openpyxl")
        dfs.append(df)

    barometer_dt_raw = dfs[0]

    # Rename columns and replace variable names
    barometer_dt = barometer_dt_raw.rename(
        columns={
            "Dossier_ID": "FileNumber",
            "sample_id": "SampleNumber",
            "farm_ID": "FarmID",
            "project": "Project",
            "date": "Date",
            "Lab_reference": "LabReference",
            "Sample_type": "SampleType",
            "Diagnostic_test": "DiagnosticTest",
        }
    )

    # Define functions for hashing
    def sha256_hash(text):
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    # Define mappings for Sample_type, Diagnostic_test, Breed, and Province
    sample_type_mapping = {
        "Autopsy": "Autopsy",
        "BAL": "BAL",
        "SWABS": "Swab",
        "OTHER": "Unknown",
    }

    diagnostic_test_mapping = {"PCR": "PCR", "Kweek": "Culture"}

    breed_mapping = {
        "beef": "Beef",
        "dairy": "Dairy",
        "mixed": "Mixed",
        "veal": "Veal",
        "other": "Unknown",
        "rearing": "Unknown",
        "unknown": "Unknown",
    }

    province_mapping = {
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
    # Perform the data manipulation using pandas
    barometer_dt = barometer_dt.assign(
        Country="The Netherlands",
        LabReference="2",
        SampleType=barometer_dt["reason_of_sampling"]
        .map(sample_type_mapping)
        .fillna("Missing"),
        DiagnosticTest=barometer_dt["test"]
        .map(diagnostic_test_mapping)
        .fillna("Missing"),
        Breed=barometer_dt["breed"].map(breed_mapping).fillna("Unknown"),
        Province=barometer_dt["provincie"]
        .map(province_mapping)
        .fillna("Missing"),
    )

    barometer_dt = barometer_dt[
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
            "Province",
            "Project",
            "FarmID",
        ]
    ]

    # Drop duplicate rows
    barometer_dt = barometer_dt.drop_duplicates()

    # Apply sha256 hashing on FileNumber, SampleNumber, and FarmID columns
    barometer_dt["FileNumber"] = barometer_dt["FileNumber"].apply(sha256_hash)
    barometer_dt["SampleNumber"] = (
        barometer_dt["SampleNumber"].astype(str).apply(sha256_hash)
    )
    barometer_dt["FarmID"] = (
        barometer_dt["FarmID"].astype(str).apply(sha256_hash)
    )
    # print(barometer_dt.head())

    barometer_dt_filtered = barometer_dt[
        (barometer_dt["Project"] == "monitoring")
        | (barometer_dt["Project"] == "no project")
    ]
    # Floor date to the 1st of the month using .loc method
    barometer_dt_filtered["Floored_date"] = (
        barometer_dt_filtered["Date"].dt.to_period("M").dt.to_timestamp()
    )
    # Aggregate data based on farm_ID and month (WIDE)
    agg_functions = {
        "PM": "max",
        "MH": "max",
        "HS": "max",
        "MB": "max",
        "BRSV": "max",
        "PI3": "max",
        "BCV": "max",
    }
    barometer_groupby = barometer_dt_filtered.groupby(
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
        observed=True
    ).agg(agg_functions)

    # Convert to LONG
    barometer_groupby.columns = [
        f"{col[0]}_{col[1]}" for col in barometer_groupby.columns
    ]
    barometer_long = pd.melt(
        barometer_groupby.reset_index(),
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
    # Save file to CSV (long version)
    # barometer_long.to_csv("output/barometer_GD.csv", index=False)

    g = rdflib.Graph()
    onto = Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    g.bind("onto", onto)
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    g.bind("xsd", xsd)

    # Iterate through the rows and create RDF triples
    for index, row in barometer_long.iterrows():
        CattleSample = onto[f'CattleSample{row["LabReference"]}_{index}']
        g.add((CattleSample, RDF.type, onto.CattleSample))

        # Handle nan values in literals
        diagnostic_test = (
            row["DiagnosticTest"] if not pd.isna(row["DiagnosticTest"]) else ""
        )
        country = row["Country"] if not pd.isna(row["Country"]) else ""
        lab_reference = (
            row["LabReference"] if not pd.isna(row["LabReference"]) else ""
        )
        sample_type = (
            row["SampleType"] if not pd.isna(row["SampleType"]) else ""
        )
        breed = row["Breed"] if not pd.isna(row["Breed"]) else ""
        Pathogen = row["Pathogen"] if not pd.isna(row["Pathogen"]) else ""
        result = row["Result"] if not pd.isna(row["Result"]) else "Missing"
        date = row["Floored_date"] if not pd.isna(row["Floored_date"]) else ""
        province = row["Province"] if not pd.isna(row["Province"]) else ""
        farm_id = row["FarmID"] if not pd.isna(row["FarmID"]) else ""

        g.add(
            (
                CattleSample,
                onto.hasDiagnosticTest,
                Literal(diagnostic_test, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasCountry,
                Literal(country, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasLabReference,
                Literal(lab_reference, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasSampleType,
                Literal(sample_type, datatype=XSD.string),
            )
        )
        g.add(
            (CattleSample, onto.hasBreed, Literal(breed, datatype=XSD.string))
        )
        g.add(
            (
                CattleSample,
                onto.hasResult,
                Literal(result, datatype=XSD.string),
            )
        )
        g.add((CattleSample, onto.hasDate, Literal(date, datatype=XSD.string)))
        g.add(
            (
                CattleSample,
                onto.hasProvince,
                Literal(province, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasFarmIdentification,
                Literal(farm_id, datatype=XSD.string),
            )
        )
        g.add(
            (
                CattleSample,
                onto.hasPathogen,
                Literal(Pathogen, datatype=XSD.string),
            )
        )

    filename_output = "RDFoutputCattleSampleLab2.ttl"
    g.serialize(destination=filename_output, format="turtle")
    return filename_output
