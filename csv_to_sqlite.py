#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Script que escreve um CSV como uma tabela num arquivo sqlite.

Uso:
    python3 csv_to_sqlite.py file.csv file.db table_name
"""

import datetime
import sqlite3
import sys

import pandas as pd


def parse_date(date: str) -> datetime.Datetime:
    """Converte as data para o formato adequado.

    Args:
        date (str): data.

    Returns:
        datetime.Datetime: data convertida.
    """
    return datetime.datetime.strptime(date, "%d/%m/%Y")

def csv_to_sqlite(csv_file: str, db_file: str, table_name: str) -> None:
    """Escreve um CSV como uma tabela num arquivo sqlite.

    Args:
        csv_file (str): caminho do arquivo CSV.
        db_file (str): caminho do arquivo SQLite.
        table_name (str): nome da tabela no arquivo.
    """
    data_frame = pd.read_csv(csv_file, sep=";")
    data_frame.columns = [
        "Região",
        "Estado",
        "Data",
        "Casos_Novos",
        "Casos_Acumulados",
        "Óbitos_Novos",
        "Óbitos_Acumulados",
    ]
    data_frame.Data = data_frame.Data.apply(parse_date)
    data_frame.groupby("Data").agg(sum).plot()
    cnx = sqlite3.connect(db_file)
    data_frame.to_sql(name=table_name, con=cnx, if_exists="replace")


if __name__ == "__main__":
    csv_to_sqlite(sys.argv[0], sys.argv[1], sys.argv[2])
