#!/usr/bin/env python3
"""
Portal tables - derived from Linear data

NOTE: portal.order is now managed by the backend (current-be/main.py) as source of truth.
This sync only handles portal.suborder (derived from Linear suborder issues).

Usage: cd current-be && uv run python scripts/sync_portal.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

DATABASE_URL = os.environ['DATABASE_URL']

SUBORDERS_PROJECT_ID = 'd5abf424-7d87-450b-b722-d08dc67a7105'

REFRESH_SQL = f'''
DROP TABLE IF EXISTS portal.suborder;

CREATE TABLE portal.suborder AS
SELECT
    i.id::uuid AS linear_id,
    i.identifier AS linear_identifier,
    -- Portal Data fields extracted into real columns:
    parsed_data->>'id' AS id,
    parsed_data->>'order_id' AS order_id,
    parsed_data->>'utilities' AS utilities,
    parsed_data->>'provider' AS provider,
    parsed_data->>'scheduled_for' AS scheduled_for,
    parsed_data->>'status' AS status
FROM linear.issues i
CROSS JOIN LATERAL (
    SELECT jsonb_object_agg(
        TRIM(split_part(line, ':', 1)),
        TRIM(substring(line FROM position(':' IN line) + 1))
    ) AS parsed_data
    FROM regexp_split_to_table(
        (regexp_match(i.description, '\\*\\*Portal Data\\*\\*\\s*\\n+```\\s*\\n([\\s\\S]*?)\\n```'))[1],
        '\\n'
    ) AS line
    WHERE line ~ '^\\s*[^:]+:'
) parsed
WHERE i.project_id = '{SUBORDERS_PROJECT_ID}';

-- Add primary key for webhook UPSERT support
ALTER TABLE portal.suborder ADD PRIMARY KEY (linear_id);
'''

def sync():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Refreshing portal.suborder...")
    cur.execute('CREATE SCHEMA IF NOT EXISTS portal')
    cur.execute(REFRESH_SQL)
    conn.commit()

    # Get count
    cur.execute('SELECT COUNT(*) FROM portal.suborder')
    suborder_count = cur.fetchone()[0]

    print(f"  portal.suborder: {suborder_count} rows")

    cur.close()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    sync()
