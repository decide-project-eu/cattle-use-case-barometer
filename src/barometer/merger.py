import time
from datetime import datetime
from itertools import count
from pathlib import Path

import pandas as pd
from rdflib import OWL, RDF, RDFS, Graph, Literal, Namespace

import barometer.graph


def turtle_file_merger(*ttl_files: Path | str):
    graph = Graph()
    for ttl_file in ttl_files:
        graph.parse(ttl_file, format="turtle")

    # queryer(graph)

    # Define filename and folder path for RDF
    folder_path_rdf = "./output/"
    base_filename = "barometer_combined"
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_filename_rdf = f"{folder_path_rdf}{base_filename}_{current_date}.rdf"

    # Save RDF graph to file
    graph.serialize(new_filename_rdf, format="turtle")


def queryer(g):
    # Use RDFS or OWL reasoning to infer additional knowledge
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)
    g.bind("onto", Namespace("http://www.purl.org/decide/LivestockHealthOnto"))

    query = """
    PREFIX onto: <http://www.purl.org/decide/LivestockHealthOnto>
    SELECT ?FarmIdentification ?DiagnosticTest ?SampleType ?Date ?Breed ?LabReference ?Pathogen ?Country ?Province ?Result
    WHERE {
      ?CattleSample onto:hasFarmIdentification ?FarmIdentification .
      ?CattleSample onto:hasDiagnosticTest ?DiagnosticTest .
      ?CattleSample onto:hasSampleType ?SampleType .
      ?CattleSample onto:hasDate ?Date .
      ?CattleSample onto:hasBreed ?Breed .
      ?CattleSample onto:hasLabreference ?LabReference .
      ?CattleSample onto:hasPathogen ?Pathogen .
      ?CattleSample onto:hasCountry ?Country .
      ?CattleSample onto:hasProvince ?Province .
      ?CattleSample onto:hasResult ?Result .
      }
    """

    # execute the query and retrieve the results
    query_start = time.perf_counter()
    results = barometer.graph.query(query)
    query_end = time.perf_counter()
    print(f"Querying graph took {query_end - query_start:.2f} seconds")

    # size_start = time.perf_counter()
    # result_size = len(results)
    # size_end = time.perf_counter()
    # print(f"Determining size took {size_end - size_start:.2f} seconds")
    # print(f"Count of rows in result set: {result_size}")

    # # convert the results to a Pandas dataframe
    data = []

    convert_start = time.perf_counter()
    for row in results:
        data.append(list(row))
    convert_end = time.perf_counter()
    print(
        f"Converting graph to Python list took {convert_end - convert_start:.2f} seconds"
    )

    assemble_start = time.perf_counter()
    df = pd.DataFrame(
        data,
        columns=[
            "FarmIdentification",
            "DiagnosticTest",
            "SampleType",
            "Date",
            "Breed",
            "LabReference",
            "Pathogen",
            "Country",
            "Province",
            "Result",
        ],
    )
    assemble_end = time.perf_counter()
    print(
        f"Dataframe assembly took {assemble_end - assemble_start:.2f} seconds"
    )

    # display the dataframe
    df.to_csv(r"output/query_result.csv")
