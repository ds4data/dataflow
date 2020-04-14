import os
from pathlib import Path
import streamlit as st
import glob

from dataflow import dataflow as df


DB_PATH = Path(os.environ["DB_PATH"])
DATAFLOW_PASSWORD = os.environ["DATAFLOW_PASSWORD"]


st.title("DS4Data")
if st.text_input("Senha: ") == DATAFLOW_PASSWORD:
    st.write("## Adição de base de dados")
    encoding = st.selectbox(
        label="Codificação", options=["utf-8", "iso-8859-1"], index=0
    )
    sep = st.text_input(label="Delimitador", value=",")
    if st.checkbox("Usar link"):
        path = st.text_input("Link")
    else:
        path = st.file_uploader(
            "Envie um arquivo CSV", type=["csv", "csv.gz"], encoding=encoding
        )
    if path:
        ds = df.DataSource("", path, sep, encoding=encoding)

        @st.cache(persist=True, allow_output_mutation=True)
        def get_data():
            if path:
                return df.pd.read_csv(path, sep=sep, encoding=encoding)
            return None

        try:
            ds.data = get_data()
            columns = list(ds.data.columns)
            if st.checkbox("Exibir dados", value=True):
                st.subheader("Dados:")
                st.write(ds.data)
            st.write("## Processamento")
            ds.drop_columns = st.multiselect(label="Ignorar Colunas", options=columns)
            columns.insert(0, "")
            date_column = st.selectbox(
                label="Coluna com data", options=columns, index=0
            )
            if date_column:
                date_format = st.text_input(label="Formato da data (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)",
                    value="%Y-%m-%d",
                )
            new_database = st.checkbox("Criar nova base de dados")
            if new_database:
                database = st.text_input("Nova Database")
            else:
                options = glob.glob(str(DB_PATH / "*.db"))
                print(options)
                database = st.selectbox("Databases existentes", options=options)
            if database:
                db_extension = ".db"
                if not database.endswith(db_extension):
                    database = database + db_extension
                ds.label = st.text_input(
                    label="Nome da tabela no banco de dados (case-insensitive)"
                ).lower()
                if_exists = st.selectbox(
                    "Em caso de tabela existente com mesmo nome",
                    options=["fail", "replace", "append"],
                    format_func=lambda x: {
                        "fail": "Avisar",
                        "replace": "Substituir",
                        "append": "Anexar",
                    }[x],
                    index=1,
                )
                if ds.label:
                    if st.button("Processar e gravar dados"):
                        ds.transform_data()
                        connection = df.sqlite3.connect(database)
                        ds.store_data(connection, if_exists=if_exists)
                        connection.close()
                        st.success(f"Dados gravados com sucesso em {ds.label}! ✅")
                        st.balloons()
                        st.write(ds.data)
        except FileNotFoundError:
            pass
