# chi-food-risk

Cloud-native data pipeline that scores every Chicago food establishment on a 0–100 risk scale using inspection history and NLP-based violation analysis.

**Live dashboard:** https://chi-food-risk.streamlit.app/

![dashboard screenshot](reports/figures/dashboard_screenshot.png)

---

## The Problem

Chicago has 42,000+ food establishments and limited health inspectors. The city assigns a static "Risk" category (1/2/3) based on facility type: restaurants are always Risk 1, prepackaged food shops are always Risk 3. This determines how often a place gets inspected, but says nothing about how it actually performs.

A restaurant that failed its last 5 inspections with rodent violations has the same risk category as one with a perfect record.

## What This Does

Computes a dynamic risk score (0–100) for every establishment based on what actually happened during inspections:

- Failure rate: weighted by inspection type (a fail during a food poisoning investigation counts more than a routine canvass fail)
- Violation severity: hybrid scoring using structured violation codes (1–14 = critical, 15–29 = serious) and NLP keyword extraction on inspector free-text comments
- Recency: recent failures score higher via exponential decay
- Trend: compares recent inspections against historical average to detect places getting worse

Results are served through a Streamlit dashboard where a non-technical stakeholder can see which neighborhoods and establishments need attention.

## Dataset

Chicago Food Inspections (from City of Chicago Open Data Portal)

- Source: https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5
- ~307K inspection records from January 2010 to present
- Updated weekly (Fridays)
- API: Socrata Open Data API (SODA)

---

## Quickstart

### 1) Setup

```
git clone https://github.com/rajansavani/chi-food-risk.git
cd chi-food-risk
python -m venv venv
venv\Scripts\Activate       # Windows
source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

### 2) Run the pipeline

```
python src/ingest.py         # pull data from Socrata API (~5 min)
python src/transform.py      # clean, parse violations, compute scores
python src/load.py           # load into Supabase (requires .env)
```

### 3) Launch the dashboard

```
streamlit run streamlit_app/app.py
```

The dashboard reads from the database if `DATABASE_URL` is set, otherwise falls back to local CSV files in `data/`.

### 4) Explore violation parsing

To see sample violation texts and what the parsers extract:

```
python src/transform.py --explore
```

---

## Environment Variables

Create a `.env` file in the repo root:

```
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@db.YOUR_PROJECT.supabase.co:5432/postgres
```

Optional (increases Socrata API rate limit):

```
SOCRATA_APP_TOKEN=your_token_here
```

---

## Risk Score Methodology

The dynamic score (0–100) is a weighted composite of four signals:

| Component          | Weight | What It Measures                                                                |
| ------------------ | ------ | ------------------------------------------------------------------------------- |
| Failure rate       | 25%    | Fraction of inspections that were fail/conditional, weighted by inspection type |
| Recency            | 25%    | Days since last failure, exponential decay (half-life = 180 days)               |
| Violation severity | 30%    | Hybrid: structured violation codes + NLP keyword matches, equal weight          |
| Trend              | 20%    | Recent inspection results vs. historical average                                |

### Violation Severity (hybrid approach)

Two signals, weighted equally (50/50):

Signal A (Structured): Violation numbers extracted via regex and classified per the city's own tiers (1–14 = critical, 15–29 = serious, 30–44/70 = minor).

Signal B (NLP keywords): Free-text inspector comments scanned for severity indicators like `mice`, `droppings`, `mold`, `temperature`, `sewage`. Keywords grouped into critical/major/minor tiers with corresponding weights based on connotation. Future work will involve training a separate NLP model that weights each word in the comments and weights it.

Raw combined score is passed through a soft-cap curve (`100 * (1 - e^(-x/10))`) to prevent a single bad inspection from maxxing out the score.

---

## Project Structure

```
chi-food-risk/
  src/
    config.py              constants, keyword dictionaries, scoring weights
    ingest.py              Socrata API data pull with pagination
    transform.py           cleaning, violation parsing, risk scoring
    load.py                database loading (Supabase PostgreSQL)
  streamlit_app/
    app.py                 interactive dashboard
  data/                    (not tracked)
    raw_inspections.csv
    transformed_inspections.csv
    risk_scores.csv
  .github/
    workflows/
      update-data.yml      weekly scheduled pipeline
  Dockerfile
  requirements.txt
  .env.example
```

---

## Docker

Build and run the full ETL pipeline in a container:

```
docker build -t chi-food-risk .
docker run -it --env-file .env chi-food-risk
```

---

## Pipeline Output

After running the pipeline:

- `data/transformed_inspections.csv`: 264K cleaned inspection records with parsed violation details
- `data/risk_scores.csv`: 42,904 establishments scored 0–100
- Supabase tables: `inspections`, `risk_scores` (with indexes for dashboard query performance)

Score distribution from the current dataset:

```
Mean:   27.6
Median: 27.8
Std:    11.4
Max:    85.7
```
