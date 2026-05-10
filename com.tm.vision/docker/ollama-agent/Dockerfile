FROM python:3.12-slim

# Cài ollama Python client
RUN pip install --no-cache-dir ollama

WORKDIR /workspace

# Copy agent scripts vào image
COPY planner.py reviewer.py ./
