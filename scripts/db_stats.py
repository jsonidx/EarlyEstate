"""
Quick DB stats — run via GitHub Actions or locally with DATABASE_URL_SYNC set.
"""
import os
import sys

import psycopg2

url = os.environ.get("DATABASE_URL_SYNC")
if not url:
    print("DATABASE_URL_SYNC not set")
    sys.exit(1)

conn = psycopg2.connect(url)
cur = conn.cursor()

queries = {
    "events (insolvency publications)": "SELECT COUNT(*) FROM event WHERE event_type='INSOLVENCY_PUBLICATION'",
    "parties (unique debtors)":         "SELECT COUNT(*) FROM party",
    "parties — COMPANY":                "SELECT COUNT(*) FROM party WHERE party_type='COMPANY'",
    "parties — UNKNOWN (consumers)":    "SELECT COUNT(*) FROM party WHERE party_type='UNKNOWN'",
    "party_addresses (geocoded)":       "SELECT COUNT(*) FROM party_address",
    "raw_documents":                    "SELECT COUNT(*) FROM raw_document",
    "asset_leads (bank portal)":        "SELECT COUNT(*) FROM asset_lead",
    "match_candidates":                 "SELECT COUNT(*) FROM match_candidate",
    "alerts (SENT)":                    "SELECT COUNT(*) FROM alert WHERE status='SENT'",
    "alerts (PENDING/DIGEST)":          "SELECT COUNT(*) FROM alert WHERE status!='SENT'",
    "match HIGH (≥80)":                "SELECT COUNT(*) FROM match_candidate WHERE score_total>=80",
    "match MEDIUM (50-79)":            "SELECT COUNT(*) FROM match_candidate WHERE score_total>=50 AND score_total<80",
    "match LOW (20-49)":               "SELECT COUNT(*) FROM match_candidate WHERE score_total>=20 AND score_total<50",
}

print("\n=== EarlyEstate DB Stats ===\n")
for label, sql in queries.items():
    cur.execute(sql)
    count = cur.fetchone()[0]
    print(f"  {label:<40} {count:>8,}")

# Top 5 states by event count
print("\n--- Events by state (top 5) ---")
cur.execute("""
    SELECT payload->>'state' as state, COUNT(*) as n
    FROM event
    WHERE event_type='INSOLVENCY_PUBLICATION'
    GROUP BY state ORDER BY n DESC LIMIT 5
""")
for row in cur.fetchall():
    print(f"  {(row[0] or 'unknown'):<30} {row[1]:>8,}")

# Date range
print("\n--- Publication date range ---")
cur.execute("""
    SELECT MIN(event_time), MAX(event_time)
    FROM event WHERE event_type='INSOLVENCY_PUBLICATION'
""")
row = cur.fetchone()
print(f"  Earliest: {row[0]}")
print(f"  Latest:   {row[1]}")

conn.close()
