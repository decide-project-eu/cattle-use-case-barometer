import logging
from datetime import datetime

import pandas as pd
import pantab

from barometer import graph
from barometer.lab import arsia, gd, ireland, pathosense, dgz
from barometer.tableau import Tableau

logger = logging.getLogger(__name__)


def process_arsia():
    arsia_input = r"data\ARSIA\ARSIA_DECIDE_20221201.xlsx"
    arsia_raw = pd.read_excel(
        arsia_input,
        engine="openpyxl",
        dtype={
            "N° échantillon": "str",
            "TRP": "str",
            "ADDRESS": "str",
            "SPECUL": "str",
            "Sample Type": "str",
            "METH": "str",
            "M_haemolytica": "Int8",
            "P_multocida": "Int8",
            "H_somnus": "Int8",
            "M_bovis": "Int8",
            "BRSV": "Int8",
            "PI3": "Int8",
            "Coronavirus": "Int8",
        },
        parse_dates=["Date of Sample"],
        na_values=["", " "],
    )
    arsia_raw["ADDRESS"] = arsia_raw["ADDRESS"].str.strip()
    arsia_preprocessed = arsia.preprocess(arsia_raw)
    arsia_graph = graph.build(arsia_preprocessed)
    arsia_results = graph.query(arsia_graph)
    arsia_final = graph.to_dataframe(arsia_results)

    arsia_preprocessed.to_csv(
        "output/arsia_preprocessed.csv", na_rep="NA", index=False
    )
    arsia_final.to_csv("output/arsia_final.csv", na_rep="NA", index=False)
    return arsia_final


def process_dgz():
    dgz_input = [
        r"data\DGZ\DECIDE_MTA_UGENT_14nov2022.xlsx",
        r"data\DGZ\DECIDE_MTA_UGENT_BAC_AERO_14nov2022.xlsx",
        r"data\DGZ\DECIDE_MTA_UGENTBAC_MYCO_14nov2022.xlsx"
    ]
    dgz_raw = [pd.read_excel(file, engine="openpyxl") for file in dgz_input]
    dgz_preprocessed = dgz.preprocess(*dgz_raw)
    dgz_graph = graph.build(dgz_preprocessed)
    dgz_results = graph.query(dgz_graph)
    dgz_final = graph.to_dataframe(dgz_results)

    dgz_preprocessed.to_csv(
        "output/dgz_preprocessed.csv", na_rep="NA", index=False
    )
    dgz_final.to_csv(
        "output/dgz_final.csv", na_rep="NA", index=False
    )
    return dgz_final


def process_dg():
    gd_input = r"data/GD/221122_data_RGD_DECIDE_nw.xlsx"
    gd_raw = pd.read_excel(
        gd_input,
        engine="openpyxl",
        dtype={
            "HS": "Int8",
            "MH": "Int8",
            "PM": "Int8",
            "BCV": "Int8",
            "MB": "Int8",
            "PI3": "Int8",
            "BRSV": "Int8",
            "sample_id": "str",
        },
    )
    gd_preprocessed = gd.preprocess(gd_raw)
    gd_graph = graph.build(gd_preprocessed)
    gd_results = graph.query(gd_graph)
    gd_final = graph.to_dataframe(gd_results)

    gd_preprocessed.to_csv(
        "output/gd_preprocessed.csv", na_rep="NA", index=False
    )
    gd_final.to_csv("output/gd_final.csv", na_rep="NA", index=False)
    return gd_final


def process_ireland():
    ireland_input = [
        r"data/Ireland/Jade_2021_Final_Anonymised_data_Only_2023-04-20.v2.xlsx",
        r"data/Ireland/Jade_2022_Final_Anonymised_data_Only_2023-04-21.xlsx",
    ]
    files = [
        pd.read_excel(
            file,
            engine="openpyxl",
            dtype={"AGE": "str"},
            keep_default_na=False,
            na_values=[""],
        )
        for file in ireland_input
    ]

    ireland_raw = pd.concat(files)
    ireland_raw["RESULT"] = ireland_raw["RESULT"].str.replace('"', "")
    ireland_preprocessed = ireland.preprocess(ireland_raw)
    ireland_graph = graph.build(ireland_preprocessed)
    ireland_results = graph.query(ireland_graph)
    ireland_final = graph.to_dataframe(ireland_results)

    ireland_preprocessed.to_csv(
        "output/ireland_preprocessed.csv", na_rep="NA", index=False
    )
    ireland_final.to_csv("output/ireland_final.csv", na_rep="NA", index=False)
    return ireland_final


def process_pathosense():
    pathosense_input = (
        r"data/PathoSense/AllBovineRespiratory_NegativesIncluded.csv"
    )
    pathosense_raw = pd.read_csv(
        pathosense_input,
        quoting=1,
        quotechar='"',
        na_filter=False,
        dtype={"sample_id": "str", "farm_id": "str"},
        parse_dates=["created"],
    )
    pathosense_preprocessed = pathosense.preprocess(pathosense_raw)
    pathosense_graph = graph.build(pathosense_preprocessed)
    pathosense_results = graph.query(pathosense_graph)
    pathosense_final = graph.to_dataframe(pathosense_results)

    pathosense_preprocessed.to_csv(
        "output/pathosense_preprocessed.csv", na_rep="NA", index=False
    )
    pathosense_final.to_csv(
        "output/pathosense_final.csv", na_rep="NA", index=False
    )
    return pathosense_final


def main():
    merged = pd.concat(
        [
            process_arsia(),
            process_dgz(),
            process_dg(),
            process_ireland(),
            process_pathosense(),
        ]
    )

    logger.info("Processing RDF query results for Hyper conversion")
    merged["Month"] = merged["Date"].dt.month.astype("Int64")
    merged["Year"] = merged["Date"].dt.year.astype("Int64")
    merged.rename(
        columns={
            "Date": "FlooredDate",
            "FarmIdentification": "FarmIdentifier"
        },
        inplace=True
    )

    merged.to_csv("output/barometer_combined.csv", na_rep="NA", quoting=1)
    logger.info("Writing RDF query results to Hyper file")
    pantab.frame_to_hyper(merged, "output/barometer_combined.hyper", table="Cattle barometer")
    logger.info("Done writing RDF query results to Hyper file")


    logger.info("Publishing Hyper file to Tableau Cloud")
    tableau = Tableau.from_conf("tableau_conf.json")
    tableau.publish_hyper(
        "output/barometer_combined.hyper",
        "decide-project-eu",
        f"Cattle barometer {datetime.today().strftime('%Y-%m-%d')}"
    )
    logger.info("Done publishing Hyper file to Tableau Cloud")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    main()
