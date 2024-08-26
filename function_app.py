import csv
import logging

import pandas as pd

from barometer import graph
from barometer.lab import arsia, gd, ireland, pathosense


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
    pass


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
            process_dg(),
            process_ireland(),
            process_pathosense(),
        ]
    )
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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    )
    main()
