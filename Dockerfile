FROM python:3.7.6

ADD . /dataflow

RUN pip install -r dataflow/requirements.txt

CMD [ "streamlit", "run", "dataflow/app.py" ]