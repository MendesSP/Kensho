## Pull the mysql:5.6 image
FROM python:3.7
 
## The maintainer name and email
MAINTAINER Andre Mendes <email>

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV EXECUTION_ID 111111
ENV DB_HOST docker.for.mac.host.internal 

RUN ipython kernel install --user --name=venv-kensho-raw-to-db

RUN mkdir notebooks
RUN mkdir notebooks/output
RUN mkdir data

COPY data data

COPY notebooks/raw_to_sql.ipynb notebooks/raw_to_sql.ipynb

COPY run.sh run.sh

RUN echo $DB_HOST

ENTRYPOINT ["bash", "run.sh"]
