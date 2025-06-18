# boat-data-etl

A simple, production-grade ETL pipeline for cleaning and validating raw boat listing data. This script:

- Cleans UTF-8 encoded CSV files with non-ASCII characters.
- Converts currency and year fields into structured formats.
- Validates schema using [Pandera](https://pandera.readthedocs.io/).
- Jupyter Notebook contains an exploratory analysis of the same data

## Usage

You can run the ETL pipeline as follows.

```bash
  boat-etl \
  -i data/boat_data.csv \
  -o output/validated_boat_data.csv
```

## Docker Usage

You can run the ETL pipeline inside a Docker container for reproducibility and ease of deployment.

### 1. Build the Docker image

```bash
docker build -t boat-etl .
```

---

### 2. Run the ETL pipeline

Run the pipeline using default parameters (as set in `CMD` of `Dockerfile`):

```bash
docker run --rm boat-etl
```

Or specify input and output paths:

```bash
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/output:/app/output \
  boat-etl \
  -i data/boat_data.csv \
  -o output/validated_boat_data.csv
```


