import os
import pandas as pd
from sqlalchemy import create_engine, text

from config import DATABASE_URL, TRANSFORMED_DATA_PATH, RISK_SCORES_PATH


def get_engine():
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL is not set. "
            "Make sure you have a .env file with your Supabase connection string."
        )

    return create_engine(DATABASE_URL)


def create_tables(engine):
    print("Creating tables...")

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS risk_scores CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS inspections CASCADE;"))

        conn.execute(text("""
            CREATE TABLE inspections (
                inspection_id       TEXT PRIMARY KEY,
                establishment_id    TEXT,
                dba_name            TEXT,
                aka_name            TEXT,
                license_            TEXT,
                facility_type       TEXT,
                risk                TEXT,
                address             TEXT,
                zip                 TEXT,
                inspection_date     TIMESTAMP,
                inspection_type     TEXT,
                results             TEXT,
                result_score        REAL,
                violations          TEXT,
                n_critical          INTEGER,
                n_serious           INTEGER,
                n_minor             INTEGER,
                kw_critical         INTEGER,
                kw_major            INTEGER,
                kw_minor            INTEGER,
                violation_severity  REAL,
                latitude            REAL,
                longitude           REAL
            );
        """))

        conn.execute(text("""
            CREATE TABLE risk_scores (
                establishment_id        TEXT PRIMARY KEY,
                dba_name                TEXT,
                aka_name                TEXT,
                address                 TEXT,
                zip                     TEXT,
                facility_type           TEXT,
                risk_category           TEXT,
                latitude                REAL,
                longitude               REAL,
                total_inspections       INTEGER,
                total_failures          INTEGER,
                total_conditional       INTEGER,
                total_passes            INTEGER,
                last_inspection_date    TIMESTAMP,
                last_result             TEXT,
                failure_rate_score      REAL,
                recency_score           REAL,
                violation_severity_score REAL,
                trend_score             REAL,
                dynamic_risk_score      REAL
            );
        """))

        # indexes for dashboard query performance
        conn.execute(text("CREATE INDEX idx_risk_score ON risk_scores (dynamic_risk_score DESC);"))
        conn.execute(text("CREATE INDEX idx_risk_zip ON risk_scores (zip);"))
        conn.execute(text("CREATE INDEX idx_insp_est ON inspections (establishment_id);"))
        conn.execute(text("CREATE INDEX idx_insp_date ON inspections (inspection_date DESC);"))

        conn.commit()

    print("  Tables and indexes created.")


def load_inspections(engine):
    """Load the transformed inspections CSV into the database."""

    print(f"Loading inspections from {TRANSFORMED_DATA_PATH}...")

    df = pd.read_csv(TRANSFORMED_DATA_PATH, low_memory=False)

    # only load columns that match our schema
    keep = [
        "inspection_id", "establishment_id", "dba_name", "aka_name",
        "license_", "facility_type", "risk", "address", "zip",
        "inspection_date", "inspection_type", "results", "result_score",
        "violations", "n_critical", "n_serious", "n_minor",
        "kw_critical", "kw_major", "kw_minor", "violation_severity",
        "latitude", "longitude",
    ]
    keep = [c for c in keep if c in df.columns]

    df[keep].to_sql("inspections", engine, if_exists="append", index=False, chunksize=5000)

    print(f"  Loaded {len(df):,} inspection records")


def load_risk_scores(engine):
    """Load the risk scores CSV into the database."""

    print(f"Loading risk scores from {RISK_SCORES_PATH}...")

    df = pd.read_csv(RISK_SCORES_PATH)
    df = df.dropna(subset=["establishment_id"])

    df.to_sql("risk_scores", engine, if_exists="append", index=False, chunksize=5000)

    print(f"  Loaded {len(df):,} establishment risk scores")


def verify(engine):
    """Quick check that the data loaded correctly."""

    print("Verifying...")

    with engine.connect() as conn:
        n_insp = conn.execute(text("SELECT COUNT(*) FROM inspections")).scalar()
        n_risk = conn.execute(text("SELECT COUNT(*) FROM risk_scores")).scalar()
        top5 = conn.execute(text(
            "SELECT dba_name, dynamic_risk_score "
            "FROM risk_scores "
            "ORDER BY dynamic_risk_score DESC "
            "LIMIT 5"
        )).fetchall()

    print(f"  inspections table: {n_insp:,} rows")
    print(f"  risk_scores table: {n_risk:,} rows")
    print(f"  Top 5 riskiest:")
    for name, score in top5:
        print(f"    {score:5.1f}  {name}")


if __name__ == "__main__":

    print("=" * 60)
    print("CHI-FOOD-RISK — DATABASE LOADING")
    print("=" * 60)
    print()

    engine = get_engine()

    # test the connection first
    print("Connecting to Supabase...")
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version();")).scalar()
        print(f"  Connected! {version[:60]}...")
    print()

    create_tables(engine)
    print()

    load_inspections(engine)
    print()

    load_risk_scores(engine)
    print()

    verify(engine)
    print()

    print("Database load complete!")