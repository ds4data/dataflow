import streamlit as st
import glob

from dataflow import dataflow as df


@st.cache(persist=True)
def get_data(path, sep, encoding):
    if path:
        ds = df.DataSource("", path, sep, encoding=encoding)
        ds.get_data()
        return ds
    return None


st.title("DS4Data")
if st.text_input("Senha: ") == "ds4data":
    st.write("## Adição de base de dados")
    encoding = st.selectbox(
        label="Codificação", options=["utf-8", "iso-8859-1"], index=0
    )
    path = st.file_uploader(
        "Choose a CSV file", type=["csv", "csv.gz"], encoding=encoding
    )
    if path:
        sep = st.text_input(label="Delimitador", value=",")
        try:
            ds = get_data(path, sep, encoding)
            columns = list(ds.data.columns)
            if st.checkbox("Exibir dados", value=True):
                st.subheader("Dados:")
                st.write(ds.data)
            st.write("## Processamento")
            drop_columns = st.multiselect(label="Ignorar Colunas", options=columns)
            columns.insert(0, "")
            date_column = st.selectbox(
                label="Coluna com data", options=columns, index=0
            )
            if date_column:
                date_format = st.text_input(label="Formato da data", value="%Y-%m-%d",)
                st.write(
                    "###### Documentação: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes"
                )
            new_database = st.checkbox("Criar nova base de dados")
            if new_database:
                database = st.text_input("Nova Database ('*.db')")
            else:
                database = st.selectbox(
                    "Databases existentes", options=glob.glob("*.db")
                )
            db_extension = ".db"
            if not database.endswith(db_extension):
                database = database + db_extension
            label = st.text_input(
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
            if label:
                if st.button("Processar e gravar dados"):
                    ds.transform_data()
                    connection = df.sqlite3.connect(database)
                    ds.store_data(connection, table_name=label, if_exists=if_exists)
                    connection.close()
                    st.success(f"Dados gravados com sucesso em {label}! ✅")
                    st.balloons()
                    st.write(ds.data)
        except FileNotFoundError:
            pass
