FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/pipeline/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipline/DF-pipeline.py"

COPY . /template

# Install pipeline dependencies locally for image validation
RUN pip install --no-cache-dir -U -r /template/pipeline/requirements.txt
