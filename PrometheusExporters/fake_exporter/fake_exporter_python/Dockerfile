FROM sketchdb-base:latest

LABEL maintainer="SketchDB Team"
LABEL description="Prometheus Client for SketchDB"

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY fake_exporter.py .

ENTRYPOINT ["python", "fake_exporter.py"]
