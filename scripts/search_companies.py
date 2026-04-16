"""
Search insolvency party companies by keyword.
Usage: DATABASE_URL_SYNC=... python scripts/search_companies.py "odoo"
"""
import os
import sys
import psycopg2

keyword = sys.argv[1] if len(sys.argv) > 1 else "odoo"
conn = psycopg2.connect(os.environ["DATABASE_URL_SYNC"])
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT ON (p.id)
           p.name_raw,
           p.party_type,
           e.payload->>'court' as court,
           e.payload->>'state' as state,
           e.payload->>'case_number' as case_number,
           e.payload->>'register_info' as register,
           e.event_time::date as pub_date
    FROM party p
    JOIN event e ON e.party_id = p.id
    WHERE p.party_type = 'COMPANY'
      AND p.name_raw ILIKE %s
    ORDER BY p.id, pub_date DESC
""", (f"%{keyword}%",))

rows = cur.fetchall()
print(f"\n=== Companies matching '{keyword}' ({len(rows)} results) ===\n")
for row in rows:
    print(f"  {row[0]}")
    print(f"    {row[3]} | {row[2]} | Case: {row[4]} | Reg: {row[5] or '-'} | {row[6]}")
    print()

conn.close()
