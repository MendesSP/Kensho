## Pull the mysql:5.6 image
FROM python:3.7
 
## The maintainer name and email
MAINTAINER Andre Mendes <email>

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV EXECUTION_ID 111111
ENV DB_HOST docker.for.mac.host.internal 

RUN ipython kernel install --user --name=venv-kensho-feature-selection

RUN mkdir notebooks
RUN mkdir notebooks/output

COPY notebooks/feature_selection.ipynb notebooks/feature_selection.ipynb
COPY notebooks/label_encoder.p notebooks/label_encoder.p

COPY run.sh run.sh


ENTRYPOINT ["bash", "run.sh"]
