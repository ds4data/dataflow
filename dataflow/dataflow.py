#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime
import sqlite3
from datetime import datetime
import yaml


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
        self.__dict__.update(content)

    def get_data(self, source: str, sep: str = ",") -> pd.DataFrame:
        """Get the date from the source.

        Args:
            source (str): source.
            sep (str): columns delimiter. Defaults to ",".

        Returns:
            pd.DataFrame: data.
        """
        print(f"Fetching data from {source} ...\n")
        data = pd.read_csv(source, sep=sep)
        print("Amostra:")
        print(data.head())
        return data

    def transform_data(
        self,
        data: pd.DataFrame,
        date_column: str = "date",
        date_format="%Y-%m-%d",
        drop_columns: list = [],
    ) -> pd.DataFrame:
        """Tranform and clean the data.

        Args:
            file (pd.DataFrame): data.
            drop_columns (List): columns to be dropped.
            date_column (str, optional): column where lies the date for the records. Defaults to "date".
            date_format (str, optional): format for the date in date_column. Defaults to "%Y-%m-%d".

        Returns:
            pd.DataFrame: transformed data.
        """
        print("Tranformming and cleaning data...")
        # data[date_column] = pd.to_datetime(data[date_column], format=date_format)
        data[date_column] = data[date_column].apply(
            lambda x: datetime.strptime(x, date_format)
        )
        data.drop(drop_columns or [], axis=1, inplace=True)
        print("Done!\b")
        return data

    def store_data(self, data: pd.DataFrame, db_file: str, table_name: str) -> None:
        """Store the dara in SQLite database.

        Args:
            data (pd.DataFrame): data.
            db_file (str): file to write the date in.
            table_name (str): table name.
        """
        print(f"\nStoring data in {self.output_file}...")
        data.to_sql(
            name=table_name, index=False, con=self.connection, if_exists="replace"
        )
        print("Done!\b")

    def run(self):
        for label, values in zip(self.sources.keys(), self.sources.values()):
            data = self.get_data(values["source"], values["sep"])
            data = self.transform_data(
                data,
                values["date_column"],
                values["date_format"],
                values["drop_columns"],
            )
            self.store_data(data, self.output_file, label)

    def __enter__(self):
        self.connection = sqlite3.connect(self.output_file)
        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()
        return self


if __name__ == "__main__":
    with DataFlow("input.yaml") as dataflow:
        dataflow.run()
