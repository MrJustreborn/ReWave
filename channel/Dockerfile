# Use an official Python base image
FROM python:3.13-slim

# Install FFmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source code
COPY src/ /app/

# Install Python dependencies (falls requirements.txt existiert)
#COPY src/requirements.txt /app/requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt found"

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "main.py"]
