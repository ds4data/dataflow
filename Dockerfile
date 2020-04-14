FROM python:3.8-slim-buster

ADD . /app

RUN pip install -r app/requirements.txt

CMD [ "streamlit", "run", "app/app.py" ]