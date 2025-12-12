#!/usr/bin/env python3
"""
Refresh all suborders from Linear.

This script fetches all suborder issues from Linear and processes them
using the same parsing logic as the webhook handler, doing a full refresh
of the portal.suborder table.

Usage: cd current-be && uv run python scripts/refresh_suborders.py
"""

import os
import re
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

# Add parent dir to path for enums import
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from enums import UTILITY_ABBREV_MAP, get_suborder_status

DATABASE_URL = os.environ['DATABASE_URL']
LINEAR_API_KEY = os.environ['LINEAR_API_KEY']
LINEAR_API_URL = 'https://api.linear.app/graphql'
SUBORDERS_PROJECT_ID = 'd5abf424-7d87-450b-b722-d08dc67a7105'


def parse_suborder_title(title: str) -> tuple[list[str], str] | None:
    """Parse 'Activate EG via Xcel Energy' → (['ELECTRIC', 'GAS'], 'Xcel Energy')"""
    match = re.match(r'Activate ([EGW]+) via (.+)', title)
    if not match:
        return None
    utilities = [UTILITY_ABBREV_MAP[c] for c in match.group(1)]
    provider = match.group(2)
    return utilities, provider


def parse_scheduled_for(description: str) -> str | None:
    """Parse 'scheduled_for: 2025-12-15' from description. Only matches YYYY-MM-DD format."""
    if not description:
        return None
    match = re.search(r'scheduled_for:\s*(\d{4}-\d{2}-\d{2})', description)
    return match.group(1) if match else None


def gql(query: str, variables: dict = None) -> dict:
    """Execute a GraphQL query against Linear API"""
    resp = requests.post(
        LINEAR_API_URL,
        json={'query': query, 'variables': variables or {}},
        headers={
            'Content-Type': 'application/json',
            'Authorization': LINEAR_API_KEY
        }
    )
    resp.raise_for_status()
    data = resp.json()
    if 'errors' in data:
        raise Exception(f"GraphQL errors: {data['errors']}")
    return data['data']


def fetch_suborders() -> list[dict]:
    """Fetch all suborder issues from Linear"""
    query = """
    query($projectId: ID!, $cursor: String) {
        issues(
            first: 100
            after: $cursor
            filter: { project: { id: { eq: $projectId } } }
        ) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                id
                identifier
                title
                description
                parent {
                    id
                }
                state {
                    name
                }
                labels {
                    nodes {
                        name
                    }
                }
            }
        }
    }
    """

    all_issues = []
    cursor = None

    while True:
        data = gql(query, {'projectId': SUBORDERS_PROJECT_ID, 'cursor': cursor})
        result = data['issues']
        all_issues.extend(result['nodes'])

        if not result['pageInfo']['hasNextPage']:
            break
        cursor = result['pageInfo']['endCursor']

    return all_issues


def refresh():
    """Refresh all suborders from Linear"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Fetch all suborders from Linear
    print("Fetching suborders from Linear...")
    issues = fetch_suborders()
    print(f"  Found {len(issues)} issues in Suborders project")

    # Process each suborder
    print("\nProcessing suborders...")
    processed = 0
    skipped_no_parent = 0
    skipped_title_invalid = 0

    for issue in issues:
        linear_id = issue['id']
        identifier = issue['identifier']
        title = issue.get('title', '')
        description = issue.get('description', '')
        parent = issue.get('parent')
        state = issue.get('state', {})
        labels = issue.get('labels', {}).get('nodes', [])

        # Check parent
        if not parent:
            skipped_no_parent += 1
            print(f"  SKIP {identifier}: no parent issue")
            continue

        order_linear_id = parent['id']

        # Parse title
        parsed = parse_suborder_title(title)
        if not parsed:
            skipped_title_invalid += 1
            print(f"  SKIP {identifier}: invalid title '{title}'")
            continue

        utilities, provider = parsed
        utilities_str = f"[{', '.join(utilities)}]"

        # Get status
        state_name = state.get('name', 'Todo') if state else 'Todo'
        label_names = [l.get('name', '') for l in labels]
        status = get_suborder_status(state_name, label_names)

        # Parse scheduled_for
        scheduled_for = parse_scheduled_for(description)

        # Upsert
        cur.execute("""
            INSERT INTO portal.suborder (linear_id, order_linear_id, utilities, provider, scheduled_for, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (linear_id) DO UPDATE SET
                order_linear_id = EXCLUDED.order_linear_id,
                utilities = EXCLUDED.utilities,
                provider = EXCLUDED.provider,
                scheduled_for = EXCLUDED.scheduled_for,
                status = EXCLUDED.status
        """, (
            linear_id,
            order_linear_id,
            utilities_str,
            provider,
            scheduled_for,
            status,
        ))
        processed += 1

    conn.commit()

    # Summary
    print(f"\nRefresh complete!")
    print(f"  Processed: {processed}")
    print(f"  Skipped (no parent): {skipped_no_parent}")
    print(f"  Skipped (invalid title): {skipped_title_invalid}")

    # Show current count
    cur.execute('SELECT COUNT(*) FROM portal.suborder')
    count = cur.fetchone()[0]
    print(f"\nTotal suborders in portal.suborder: {count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    refresh()
