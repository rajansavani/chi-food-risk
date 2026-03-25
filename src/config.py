import os
from dotenv import load_dotenv

load_dotenv()

# Socrata API (how we pull data from the City of Chicago)
SOCRATA_ENDPOINT = "https://data.cityofchicago.org/resource/4ijn-s7e5.json"
SOCRATA_PAGE_SIZE = 50_000
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN") # optional, not needed



# raw  = exactly what came from the API, untouched
# transformed = cleaned + violations parsed + per-inspection severity scored
# scores = one row per establishment with a final 0-100 risk score
RAW_DATA_PATH = "data/raw_inspections.csv"
TRANSFORMED_DATA_PATH = "data/transformed_inspections.csv"
RISK_SCORES_PATH = "data/risk_scores.csv"


# we will load results into Supabase
DATABASE_URL = os.getenv("DATABASE_URL", "")

# map results column to numeric scores for easier calculations later
RESULT_SCORES = {
    "pass":               0.0,
    "pass w/ conditions":  0.5,
    "fail":               1.0,
    "no entry":           None,
    "not ready":          None,
    "out of business":    None,
    "business not located": None,
}

# city of Chicago defines violation numbers 1-14 as "critical", 15-29 as "serious", and 30-44 and 70 as "minor"
# we will use it as one of our scoring signals
CRITICAL_VIOLATION_NUMS = set(range(1, 15))       # {1, 2, ..., 14}
SERIOUS_VIOLATION_NUMS  = set(range(15, 30))      # {15, 16, ..., 29}
MINOR_VIOLATION_NUMS    = set(range(30, 45)) | {70}  # {30, ..., 44, 70}


# key word dictionary is our second scoring signal
# left a bit hacky for now until I can train a proper NLP model
KEYWORD_TIERS = {
    "critical": [
        # rodents / vermin
        "rodent", "rodents", "mice", "mouse", "rat", "rats",
        "roach", "roaches", "cockroach", "cockroaches",
        # contamination
        "sewage", "toxic", "poisonous", "vomit",
        "feces", "fecal", "droppings", "urine",
        # pathogens
        "e. coli", "e.coli", "salmonella", "listeria", "norovirus",
    ],
    "major": [
        # spoilage / temperature abuse
        "mold", "moldy", "spoiled", "rotten", "expired", "rancid",
        "temperature", "cold holding", "hot holding", "danger zone",
        # cross-contamination
        "contaminated", "cross-contamination", "cross contamination",
        "raw meat", "undercooked",
        # hygiene failures
        "hand washing", "handwashing", "bare hands",
        # chemicals in wrong places
        "pesticide", "chemical", "sanitizer",
        # insects
        "fly", "flies", "insect", "insects", "pest", "pests",
    ],
    "minor": [
        # cleanliness / maintenance
        "dust", "dusty", "clutter", "cluttered",
        "floor", "wall", "ceiling", "ventilation",
        "lighting", "plumbing", "leak", "leaking",
        "garbage", "trash", "waste", "debris",
        # procedural
        "label", "labeling", "signage",
        "hair restraint", "hair net", "gloves",
    ],
}

# how much each tier contributes to the severity score
KEYWORD_WEIGHTS = {
    "critical": 3.0,
    "major":    2.0,
    "minor":    1.0,
}

# same weights applied to the structured violation numbers
VIOLATION_NUM_WEIGHTS = {
    "critical": 3.0,   # violations 1-14
    "serious":  2.0,   # violations 15-29
    "minor":    1.0,   # violations 30-44, 70
}



# add weight based on how the inspection was triggered (ex. a failure on a complaint inspection is worse than a failure on a routine inspection)
INSPECTION_TYPE_WEIGHTS = {
    "suspect food poisoning": 2.0,   # someone reported getting sick here
    "complaint":              1.5,   # someone filed a complaint
    "task-force":             1.3,   # targeted bar/tavern inspection
    "canvass re-inspection":  1.1,   # follow-up to a previous issue
    "canvass":                1.0,   # routine scheduled inspection
    "license":                0.8,   # pre-opening, less about ongoing risk
    "consultation":           0.5,   # advisory visit, not enforcement
}
INSPECTION_TYPE_WEIGHT_DEFAULT = 1.0  # for any type not listed above


# dynamic risk score formula is a weighted average of these components
# each is individually scaled to 0-100, then blended using these weights:
RISK_SCORE_WEIGHTS = {
    "failure_rate":       0.25, # what fraction of their inspections were fail / conditional (weighted by inspection type)
    "recency":            0.25, # how recently did they last fail? (uses exponential decay)
    "violation_severity": 0.30, # how bad were the violations (structured violation numbers + NLP keyword scores, equal weight)
    "trend":              0.20, # are they getting better or worse? (compares recent inspections against historical average)
}

# for the trend component: how many recent inspections to compare against the historical average
TREND_WINDOW = 5

# for the recency component: how fast does a failure's impact decay?
# a half-life of 180 days means a failure from 6 months ago contributes half as much as a failure today.
RECENCY_HALF_LIFE_DAYS = 180