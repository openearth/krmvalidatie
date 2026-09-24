# Standalone image for the Waterinfo downloader.
# Deliberately independent from the infra/Dockerfile validator image.

FROM python:3.11-slim AS runtime

WORKDIR /work

RUN useradd --create-home --shell /bin/bash appuser

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

USER appuser

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "download.py"]


# =============================================================================
# JupyterLab stage - interactive experimenting with the same dependencies
# =============================================================================
FROM runtime AS notebook

USER root
COPY requirements-notebook.txt /tmp/requirements-notebook.txt
RUN pip install --no-cache-dir -r /tmp/requirements-notebook.txt \
    && rm /tmp/requirements-notebook.txt

USER appuser

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
