FROM sketchdb-base:latest

LABEL maintainer="SketchDB Team"
LABEL description="ArroyoSketch pipeline configuration service"

# Set working directory
WORKDIR /app

# Install Python dependencies
RUN pip3 install --no-cache-dir jinja2 requests loguru pyyaml

# Copy application code
COPY utils/ ./utils/
COPY templates/ ./templates/
COPY examples/ ./examples/
COPY run_arroyosketch.py .
COPY delete_pipeline.py .
COPY validate_udfs.py .

# Create output directory
RUN mkdir -p /app/output

# Set the entry point
ENTRYPOINT ["python", "run_arroyosketch.py"]
