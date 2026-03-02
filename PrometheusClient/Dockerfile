FROM sketchdb-base:latest

LABEL maintainer="SketchDB Team"
LABEL description="Prometheus Client for SketchDB"

# Copy requirements and install Python dependencies
COPY . .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y sudo

# Run with host network access for communicating with host services
ENTRYPOINT ["python", "main_prometheus_client.py"]
