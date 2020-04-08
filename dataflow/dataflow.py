#!/usr/bin/env python
# coding: utf-8
import sqlite3
import sys

import pandas as pd
import yaml


class DataSource:
    def __init__(
        self,
        label,
        path,
        sep=",",
        drop_columns=[],
        date_column=None,
        date_format="%Y-%m-%d",
        encoding="utf-8",
    ):
        """Handler for Source for data in plain or compressed CSV.

        Args:
            label ([type]): [description]
            path (str): path.
            sep (str): columns delimiter. Defaults to ",".
            drop_columns (list, optional): columns to be dropped.. Defaults to [].
            date_column (str, optional): [description]. Defaults to None.
            date_format (str, optional): [description]. Defaults to "%Y-%m-%d".
            encoding (str, optional): [description]. Defaults to "utf-8".
        """
        self.label = label
        self.path = path
        self.sep = sep
        self.drop_columns = drop_columns or []
        self.date_column = date_column
        self.date_format = date_format
        self.encoding = encoding
        self.data = pd.DataFrame()

    def get_data(self) -> None:
        """Get the date from the path."""
        print(f"Fetching data from {self.path} ...\n")
        self.data = pd.read_csv(self.path, sep=self.sep, encoding=self.encoding)
        print("Amostra:")
        print(self.data.head())

    def transform_data(self) -> None:
        """Tranform and clean the data."""
        print("Tranformming and cleaning data...")
        if self.date_column:
            dates = pd.to_datetime(self.data[self.date_column], format=self.date_format)
            self.data[self.date_column] = dates.apply(lambda x: str(x.to_datetime64()))
            self.data.drop(self.drop_columns, axis=1, inplace=True)
        print("Done!\b")

    def store_data(self, db_connection, table_name=None, if_exists="replace") -> None:
        """Store the dara in SQLite database.

        Args:
            db_file (str): file to write the date in.
            table_name (str): table name. Defaults to the data source label.
        """
        print(f"\nStoring data in the database...")
        self.data.to_sql(
            name=table_name or self.label,
            con=db_connection,
            index=False,
            if_exists=if_exists,
        )
        print("Done!\b")


class DataFlow:
    """Data fetch, transformation and storing workflow."""

    def __init__(self, input_file):
        """Construtor.

        Args:
            input_file (str): path to input file (YAML).
        """
        with open(input_file, "r") as stream:
            try:
                content = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        self.output_file = content["output_file"]
        self.sources = [
            DataSource(label, **values)
            for label, values in zip(
                content["sources"].keys(), content["sources"].values()
            )
        ]

    def __enter__(self):
        self.connection = sqlite3.connect(self.output_file)
        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()
        return self

    def run(self):
        for source in self.sources:
            source.get_data()
            source.transform_data()
            source.store_data(self.connection)


if __name__ == "__main__":
    with DataFlow(sys.argv[1]) as dataflow:
        dataflow.run()
