# Build: docker build -t chi-food-risk .

# Run (pass your database connection string): docker run --env-file .env chi-food-risk

# What happens when you run it:
#   1. Pulls ~300K inspection records from the Chicago Open Data API
#   2. Cleans the data, parses violations, computes risk scores
#   3. Loads everything into your Supabase PostgreSQL database

FROM python:3.11-slim

WORKDIR /app

# install dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy source code
COPY src/ src/

# create the data directory for intermediate files
RUN mkdir -p data

# run the full pipeline
CMD ["bash", "-c", "python src/ingest.py && python src/transform.py && python src/load.py"]