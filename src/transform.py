import os
import re
import sys
import math
import pandas as pd

from config import (
    RAW_DATA_PATH,
    TRANSFORMED_DATA_PATH,
    RISK_SCORES_PATH,
    RESULT_SCORES,
    CRITICAL_VIOLATION_NUMS,
    SERIOUS_VIOLATION_NUMS,
    MINOR_VIOLATION_NUMS,
    VIOLATION_NUM_WEIGHTS,
    KEYWORD_TIERS,
    KEYWORD_WEIGHTS,
    INSPECTION_TYPE_WEIGHTS,
    INSPECTION_TYPE_WEIGHT_DEFAULT,
    RISK_SCORE_WEIGHTS,
    TREND_WINDOW,
    RECENCY_HALF_LIFE_DAYS,
)

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw inspections data.

    This function does the following:
    - Parse inspection_date into a proper datetime
    - Drop rows with unusable results (out of business, no entry, etc.)
    - Map result strings to numeric scores (pass=0, conditional=0.5, fail=1)
    - Deduplicate by inspection_id
    - Normalize text columns for consistent matching later
    - Build a stable establishment_id for grouping
    """

    print("STEP 1: Cleaning")
    print("-" * 40)

    # parse dates, drop rows with unparseable dates
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    n_bad_dates = df["inspection_date"].isna().sum()
    if n_bad_dates > 0:
        print(f"  Dropped {n_bad_dates:,} rows with unparseable dates")
    df = df.dropna(subset=["inspection_date"])

    # normalize result strings (strip whitespace, lowercase)
    df["results"] = df["results"].astype(str).str.strip().str.lower()

    # keep only rows where the result maps to a numeric score (not None).
    # drop the unusable ones (out of  business, no entry, etc.)
    usable_results = {k for k, v in RESULT_SCORES.items() if v is not None}
    before = len(df)
    df = df[df["results"].isin(usable_results)].copy()
    dropped = before - len(df)
    print(f"  Dropped {dropped:,} rows with unusable results (out of business, no entry, etc.)")
    print(f"  Remaining: {len(df):,} inspections")

    # map results to numeric scores
    df["result_score"] = df["results"].map(RESULT_SCORES)

    # city's documentation warns about potential duplicates, so we deduplicate by inspection_id if it exists
    if "inspection_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["inspection_id"], keep="last")
        dupes = before - len(df)
        if dupes > 0:
            print(f"  Removed {dupes:,} duplicate inspection records")

    # normalize text columns
    df["inspection_type"] = df["inspection_type"].astype(str).str.strip().str.lower()
    df["dba_name"] = df["dba_name"].astype(str).str.strip()
    df["aka_name"] = df.get("aka_name", pd.Series(dtype=str)).astype(str).str.strip()
    df["address"] = df["address"].astype(str).str.strip()
    df["facility_type"] = df.get("facility_type", pd.Series(dtype=str)).astype(str).str.strip()
    df["risk"] = df.get("risk", pd.Series(dtype=str)).astype(str).str.strip()
    df["zip"] = df.get("zip", pd.Series(dtype=str)).astype(str).str.strip()

    # fill missing violations with empty string
    df["violations"] = df["violations"].fillna("")

    # ensure numeric lat/long
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    # build a stable establishment identifier
    # license_ is the canonical ID from the city
    # some records might be missing it, so we fall back to name + address as a composite key
    df["license_"] = df["license_"].astype(str).str.strip()
    df["establishment_id"] = df["license_"].where(
        df["license_"].str.len() > 0,
        df["dba_name"].str.lower() + "|" + df["address"].str.lower(),
    )

    n_establishments = df["establishment_id"].nunique()
    print(f"  Unique establishments: {n_establishments:,}")
    print()

    return df


def extract_violation_numbers(text: str) -> list[int]:
    """Extract violation numbers from the violations text using regex."""

    if not text or not isinstance(text, str):
        return []

    matches = re.findall(r'(?:^|\|)\s*(\d{1,2})\.\s', text)

    # convert to ints and filter to valid violation numbers (1-44, 70)
    valid = set(range(1, 45)) | {70}
    return [int(n) for n in matches if int(n) in valid]


def classify_violation_numbers(numbers: list[int]) -> dict:
    """Count how many violation numbers fall into each severity tier."""
    return {
        "n_critical": sum(1 for n in numbers if n in CRITICAL_VIOLATION_NUMS),
        "n_serious":  sum(1 for n in numbers if n in SERIOUS_VIOLATION_NUMS),
        "n_minor":    sum(1 for n in numbers if n in MINOR_VIOLATION_NUMS),
    }


def scan_keywords(text: str) -> dict:
    """Scan violation text for NLP severity keywords."""

    if not text or not isinstance(text, str):
        return {"kw_critical": 0, "kw_major": 0, "kw_minor": 0, "kw_found": []}

    text_lower = text.lower()
    counts = {"kw_critical": 0, "kw_major": 0, "kw_minor": 0}
    found = []

    for tier_name, keywords in KEYWORD_TIERS.items():
        key = f"kw_{tier_name}"
        for keyword in keywords:
            if keyword in text_lower:
                counts[key] += 1
                found.append(keyword)

    counts["kw_found"] = found
    return counts


def compute_violation_severity(row: pd.Series) -> float:
    """
    Combine both signals into a single violation severity score (0-100).

    Signal A (structured): weighted count of violation numbers by tier
    Signal B (NLP keywords): weighted count of keyword matches by tier

    Both signals are weighted equally (50/50), then the combined
    raw score is passed through a curve that maps it to 0-100.

    Formula:  severity = 100 * (1 - e^(-raw / 10))
    """

    # signal A: structured violation numbers
    structured = (
        row.get("n_critical", 0) * VIOLATION_NUM_WEIGHTS["critical"]
        + row.get("n_serious", 0) * VIOLATION_NUM_WEIGHTS["serious"]
        + row.get("n_minor", 0) * VIOLATION_NUM_WEIGHTS["minor"]
    )

    # signal B: NLP keyword matches
    nlp = (
        row.get("kw_critical", 0) * KEYWORD_WEIGHTS["critical"]
        + row.get("kw_major", 0) * KEYWORD_WEIGHTS["major"]
        + row.get("kw_minor", 0) * KEYWORD_WEIGHTS["minor"]
    )

    # equal weight blend
    raw = (structured + nlp) / 2.0

    # soft-cap curve
    severity = 100.0 * (1.0 - math.exp(-raw / 10.0))

    return round(min(severity, 100.0), 2)


def parse_violations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run both violation parsing signals on every inspection row.

    Adds these columns to the dataframe:
        n_critical, n_serious, n_minor  — from violation numbers
        kw_critical, kw_major, kw_minor — from NLP keyword scan
        kw_found                        — list of matched keywords
        violation_severity              — combined 0-100 score
    """

    print("STEP 2: Parsing violations")
    print("-" * 40)

    # signal A: extract and classify violation numbers
    print("  Extracting violation numbers (regex)...")
    violation_nums = df["violations"].apply(extract_violation_numbers)
    num_classes = violation_nums.apply(classify_violation_numbers).apply(pd.Series)
    df = pd.concat([df, num_classes], axis=1)

    total_nums = violation_nums.apply(len).sum()
    print(f"    Found {total_nums:,} violation numbers across all inspections")

    # signal B: NLP keyword scan
    print("  Scanning for severity keywords (NLP)...")
    kw_results = df["violations"].apply(scan_keywords).apply(pd.Series)
    df = pd.concat([df, kw_results], axis=1)

    total_kw = df["kw_found"].apply(len).sum()
    print(f"    Found {total_kw:,} keyword matches across all inspections")

    # combine into per-inspection severity score
    print("  Computing per-inspection violation severity scores...")
    df["violation_severity"] = df.apply(compute_violation_severity, axis=1)

    print(f"    Mean severity: {df['violation_severity'].mean():.1f}")
    print(f"    Median severity: {df['violation_severity'].median():.1f}")
    print(f"    Max severity: {df['violation_severity'].max():.1f}")
    print()

    return df

def compute_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a dynamic 0-100 risk score for each establishment.
 
    Groups all inspections by establishment_id, then computes four
    component scores and blends them using the weights from config.py:
      1. failure_rate (25%) — weighted fail rate across all inspections
      2. recency (25%) — how recently did they last fail
      3. violation_severity (30%) — avg severity across inspections
      4. trend (20%) — are recent inspections better or worse
    """
 
    print("STEP 3: Computing risk scores")
    print("-" * 40)
 
    now = pd.Timestamp.now()
    records = []
 
    grouped = df.groupby("establishment_id")
    total_groups = len(grouped)
    print(f"  Scoring {total_groups:,} establishments...")
 
    for est_id, group in grouped:
        group = group.sort_values("inspection_date")
        if len(group) == 0:
            continue

        # component 1: failure rate (weighted by inspection type)
        type_weights = group["inspection_type"].map(
            INSPECTION_TYPE_WEIGHTS
        ).fillna(INSPECTION_TYPE_WEIGHT_DEFAULT)
 
        weighted_failures = (group["result_score"] * type_weights).sum()
        total_weight = type_weights.sum()
        failure_rate = weighted_failures / total_weight if total_weight > 0 else 0
        failure_rate_score = failure_rate * 100
 
        # component 2: recency
        # formula: recency = 100 * e^(-0.693 * days / half_life)
        failures = group[group["result_score"] > 0]
        if len(failures) > 0:
            days_since = (now - failures["inspection_date"].max()).days
            recency_score = 100.0 * math.exp(
                -0.693 * days_since / RECENCY_HALF_LIFE_DAYS
            )
        else:
            recency_score = 0.0  # never failed = no recency signal
 
        # component 3: violation severity
        # average of the per-inspection violation severity scores we computed in step 2
        severity_score = group["violation_severity"].mean()
 
        # component 4: trend
        # compare the average result_score of the last N inspections against the overall average.
        # if recent inspections are worse -> score < 50, if better -> score > 50
    
        # need at least 3 inspections to compute a meaningful trend.
        if len(group) >= 3:
            recent_avg = group.tail(TREND_WINDOW)["result_score"].mean()
            overall_avg = group["result_score"].mean()
            trend_diff = recent_avg - overall_avg
            # scale to 0-100 centered at 50
            trend_score = 50 + (trend_diff * 50)
            trend_score = max(0, min(100, trend_score))
        else:
            trend_score = 50.0  # not enough data, assume neutral
 
        # blend into final composite score
        composite = (
            RISK_SCORE_WEIGHTS["failure_rate"] * failure_rate_score
            + RISK_SCORE_WEIGHTS["recency"] * recency_score
            + RISK_SCORE_WEIGHTS["violation_severity"] * severity_score
            + RISK_SCORE_WEIGHTS["trend"] * trend_score
        )
        composite = round(max(0, min(100, composite)), 1)
 
        # grab latest inspection info for display in the dashboard
        latest = group.iloc[-1]
 
        records.append({
            "establishment_id": est_id,
            "dba_name": latest["dba_name"],
            "aka_name": latest["aka_name"],
            "address": latest["address"],
            "zip": latest["zip"],
            "facility_type": latest["facility_type"],
            "risk_category": latest["risk"],
            "latitude": latest["latitude"],
            "longitude": latest["longitude"],
            "total_inspections": len(group),
            "total_failures": int((group["result_score"] == 1.0).sum()),
            "total_conditional": int((group["result_score"] == 0.5).sum()),
            "total_passes": int((group["result_score"] == 0.0).sum()),
            "last_inspection_date": latest["inspection_date"],
            "last_result": latest["results"],
            # component scores (useful for the dashboard drill-down)
            "failure_rate_score": round(failure_rate_score, 1),
            "recency_score": round(recency_score, 1),
            "violation_severity_score": round(severity_score, 1),
            "trend_score": round(trend_score, 1),
            "dynamic_risk_score": composite,
        })
 
    risk_df = pd.DataFrame(records)
 
    # print summary stats
    print(f"  Scored {len(risk_df):,} establishments")
    print(f"  Score distribution:")
    print(f"    Mean:   {risk_df['dynamic_risk_score'].mean():.1f}")
    print(f"    Median: {risk_df['dynamic_risk_score'].median():.1f}")
    print(f"    Std:    {risk_df['dynamic_risk_score'].std():.1f}")
    print(f"  Top 10 riskiest:")
    for _, row in risk_df.nlargest(10, "dynamic_risk_score").iterrows():
        print(f"    {row['dynamic_risk_score']:5.1f}  {row['dba_name'][:40]:<40s}  {row['address']}")
    print()
 
    return risk_df


def explore_violations(df: pd.DataFrame) -> None:
    """Print sample violation texts so we can see what we're working with."""

    print()
    print("=" * 60)
    print("EXPLORING VIOLATION TEXT")
    print("=" * 60)

    # show 5 random failed inspections with violation text
    failures = df[(df["results"] == "fail") & (df["violations"].str.len() > 50)]
    samples = failures.sample(n=min(5, len(failures)), random_state=42)

    for i, (_, row) in enumerate(samples.iterrows(), 1):
        print(f"\n--- Sample {i}: {row['dba_name']} ({row['inspection_date'].date()}) ---")
        print(f"Result: {row['results']}")
        print(f"Risk category: {row['risk']}")
        print(f"Facility type: {row['facility_type']}")

        # show first 500 chars of violations
        viol_text = row["violations"][:500]
        print(f"Violations (first 500 chars):\n  {viol_text}")

        # show what our parsers extracted
        nums = extract_violation_numbers(row["violations"])
        classes = classify_violation_numbers(nums)
        kw = scan_keywords(row["violations"])

        print(f"Violation numbers found: {nums}")
        print(f"  → Critical: {classes['n_critical']}, Serious: {classes['n_serious']}, Minor: {classes['n_minor']}")
        print(f"Keywords found: {kw['kw_found']}")
        print(f"  → Critical: {kw['kw_critical']}, Major: {kw['kw_major']}, Minor: {kw['kw_minor']}")

        severity = compute_violation_severity(pd.Series({**classes, **{k: v for k, v in kw.items() if k != 'kw_found'}}))
        print(f"Violation severity score: {severity}/100")

    print()
    print("=" * 60)

if __name__ == "__main__":
 
    print("=" * 60)
    print("CHI-FOOD-RISK — DATA TRANSFORMATION")
    print("=" * 60)
    print()
 
    # load raw data
    print(f"Loading raw data from {RAW_DATA_PATH}...")
    df = pd.read_csv(RAW_DATA_PATH, low_memory=False)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    print()
 
    # step 1: clean
    df = clean(df)
 
    # step 2: parse violations
    df = parse_violations(df)
 
    # explore mode: show sample violation texts
    if "--explore" in sys.argv:
        explore_violations(df)
    else:
        # step 3: compute risk scores
        risk_df = compute_risk_scores(df)
 
        # save cleaned inspections
        save_cols = [c for c in df.columns if c != "kw_found"]
        os.makedirs(os.path.dirname(TRANSFORMED_DATA_PATH), exist_ok=True)
        df[save_cols].to_csv(TRANSFORMED_DATA_PATH, index=False)
        print(f"Saved transformed inspections to {TRANSFORMED_DATA_PATH}")
 
        # save risk scores
        risk_df.to_csv(RISK_SCORES_PATH, index=False)
        print(f"Saved risk scores to {RISK_SCORES_PATH}")
 
    print("Done!")