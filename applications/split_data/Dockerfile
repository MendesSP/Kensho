## Pull the mysql:5.6 image
FROM python:3.7
 
## The maintainer name and email
MAINTAINER Andre Mendes <email>

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV EXECUTION_ID 111111

RUN ipython kernel install --user --name=venv-kensho-split-data

RUN mkdir notebooks
RUN mkdir notebooks/output

COPY notebooks/split_data.ipynb notebooks/split_data.ipynb

COPY run.sh run.sh

ENTRYPOINT ["bash", "run.sh"]
