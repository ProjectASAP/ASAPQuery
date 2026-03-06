FROM sketchdb-base:latest

LABEL maintainer="SketchDB Team"
LABEL description="Main Controller for SketchDB"

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY classes/ ./classes/
COPY utils/ ./utils/
COPY main_controller.py .

# Create directories for input/output
RUN mkdir -p /app/input /app/output

# Set the entry point
ENTRYPOINT ["python", "main_controller.py"]