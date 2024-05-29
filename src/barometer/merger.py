from datetime import datetime
from pathlib import Path

import pandas as pd
from rdflib import Graph, Literal


def turtle_file_merger(*ttl_files: Path | str):
    graph = Graph()
    for ttl_file in ttl_files:
        graph.parse(ttl_file, format="turtle")

    # Define filename and folder path for RDF
    folder_path_rdf = "./"
    base_filename = "barometer_combined"
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_filename_rdf = f"{folder_path_rdf}{base_filename}_{current_date}.rdf"

    # Save RDF graph to file
    graph.serialize(new_filename_rdf, format="turtle")
