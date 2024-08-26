import numpy as np
import pandas as pd
from pandas import DataFrame
from rdflib import OWL, RDFS, XSD, Graph, Literal, Namespace
from rdflib.query import Result

from barometer.lab.arsia import logger
from main import logger


def build(dataframe: DataFrame) -> Graph:
    logger.info("Building RDF graph")
    logger.debug("Size of dataframe: %s rows", dataframe.size)

    # Graph creation
    g = Graph()
    onto = Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    g.bind("onto", onto)
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    g.bind("xsd", xsd)

    # Iterate through the rows of the barometer_long dataframe and create RDF triples
    for index, row in dataframe.iterrows():
        # Create a URI for the CattleSample based on the index
        cattle_sample = onto[f"CattleSample_{index}"]

        # Add triples for each attribute in the row
        g.add(
            (
                cattle_sample,
                onto.hasDiagnosticTest,
                Literal(row["diagnostic_test"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasCountry,
                Literal(row["country"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasBreed,
                Literal(row["breed"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasDate,
                Literal(row["floored_date"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasProvince,
                Literal(row["province"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasFarmIdentification,
                Literal(row["farm_id"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasSampleType,
                Literal(row["sample_type"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasPathogen,
                Literal(row["pathogen"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasResult,
                Literal(row["result"], datatype=XSD.string),
            )
        )
        g.add(
            (
                cattle_sample,
                onto.hasLabreference,
                Literal(row["lab_reference"], datatype=XSD.string),
            )
        )

    logger.debug("Size of RDF graph: %s nodes", len(g))
    logger.info("Done building RDF graph")
    return g
    # filename_output = "output/RDFOutputArsia.ttl"
    # g.serialize(destination=filename_output, format="turtle")
    # return filename_output


def query(graph: Graph) -> Result:
    # Query RDF graph
    logger.info("Querying RDF graph")
    graph.bind("rdfs", RDFS)
    graph.bind("owl", OWL)
    graph.bind(
        "onto", Namespace("http://www.purl.org/decide/LivestockHealthOnto")
    )
    query = """
        PREFIX onto: <http://www.purl.org/decide/LivestockHealthOnto>
        SELECT ?LabReference
               ?Country
               ?Breed
               ?Date
               ?Province
               ?FarmIdentification
               ?DiagnosticTest
               ?SampleType
               ?Pathogen
               ?Result
        WHERE {
          ?CattleSample onto:hasLabreference ?LabReference .
          ?CattleSample onto:hasCountry ?Country .
          ?CattleSample onto:hasBreed ?Breed .
          ?CattleSample onto:hasDate ?Date .
          ?CattleSample onto:hasProvince ?Province .
          ?CattleSample onto:hasFarmIdentification ?FarmIdentification .
          ?CattleSample onto:hasDiagnosticTest ?DiagnosticTest .
          ?CattleSample onto:hasSampleType ?SampleType .
          ?CattleSample onto:hasPathogen ?Pathogen .
          ?CattleSample onto:hasResult ?Result .
          }
    """
    results = graph.query(query)
    # logger.debug("Size of query result: %s nodes", len(results))
    logger.info("Done querying RDF graph")
    return results


def to_dataframe(results):
    # convert the results to a Pandas dataframe
    logger.info("Converting query result into dataframe")
    data = [list(row) for row in results]
    df = DataFrame(
        data,
        columns=[
            "LabReference",
            "Country",
            "Breed",
            "Date",
            "Province",
            "FarmIdentification",
            "DiagnosticTest",
            "SampleType",
            "Pathogen",
            "Result",
        ],
    )
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.replace(to_replace="<NA>", value=np.nan)

    logger.debug("Size of query result dataframe: %s rows", df.size)
    logger.info("Done converting query result into dataframe")
    return df
