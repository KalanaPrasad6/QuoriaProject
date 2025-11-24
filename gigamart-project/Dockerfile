FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

WORKDIR /dataflow
COPY . /dataflow

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/dataflow:${PYTHONPATH}"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
