#!/usr/bin/env python3
"""
Mirror orders from portal.order to Linear.

Goes through each order in portal.order, fetches the current Linear issue,
shows any discrepancies, and prompts before updating.

Usage: cd current-be && uv run python scripts/mirror_orders_from_db_to_linear.py
"""

import os
import httpx
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

# Add parent dir to path for enums import
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from enums import REASON_DISPLAY

DATABASE_URL = os.environ['DATABASE_URL']
LINEAR_API_KEY = os.environ['LINEAR_API_KEY']
LINEAR_API_URL = "https://api.linear.app/graphql"

FETCH_ISSUE_QUERY = """
    query GetIssue($id: String!) {
        issue(id: $id) {
            id
            identifier
            title
            description
            dueDate
            priority
        }
    }
"""

UPDATE_MUTATION = """
    mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
        issueUpdate(id: $id, input: $input) {
            success
            issue { id identifier }
        }
    }
"""


def build_linear_description(order_id: str, yardi_id: str, utilities: list[str], reason: str,
                              is_urgent: bool, requested_at: str, requested_for: str | None,
                              special_instructions: str | None) -> str:
    """Build Portal Data description for Linear issue"""
    return f"""+++ **Portal Data**

```
type: Order
id: {order_id}
requested_at: {requested_at}
yardi_id: {yardi_id}
utilities: [{", ".join(utilities)}]
reason: {reason}
is_urgent: {str(is_urgent).lower()}
requested_for: {requested_for or "null"}
special_instructions: {special_instructions or "null"}
```

+++"""


def parse_utilities(utilities_str: str) -> list[str]:
    """Parse utilities from '[ELECTRIC, GAS]' format to list"""
    if not utilities_str:
        return []
    cleaned = utilities_str.strip("[]")
    if not cleaned:
        return []
    return [u.strip() for u in cleaned.split(",")]


def fetch_linear_issue(client: httpx.Client, linear_id: str) -> dict | None:
    """Fetch current Linear issue data"""
    response = client.post(
        LINEAR_API_URL,
        json={"query": FETCH_ISSUE_QUERY, "variables": {"id": linear_id}},
        headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
    )
    result = response.json()
    if "errors" in result:
        return None
    return result.get("data", {}).get("issue")


def sync():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Get all orders with their property address
    cur.execute("""
        SELECT
            o.id,
            o.linear_id,
            o.yardi_id,
            o.utilities,
            o.reason,
            o.requested_at,
            o.requested_for,
            o.special_instructions,
            p.address__addr1 as street
        FROM portal."order" o
        LEFT JOIN propify.property p ON o.yardi_id = p.foreign_db_code
        WHERE o.linear_id IS NOT NULL
        ORDER BY o.created_at DESC
    """)
    orders = cur.fetchall()

    print(f"Found {len(orders)} orders with Linear IDs\n")

    updated = 0
    skipped = 0

    with httpx.Client() as client:
        for row in orders:
            order_id = str(row[0])
            linear_id = str(row[1])
            yardi_id = row[2]
            utilities_str = row[3]
            reason = row[4]
            requested_at = row[5].isoformat() if row[5] else None
            requested_for = str(row[6]) if row[6] else None
            special_instructions = row[7]
            street = row[8]

            if not street:
                print(f"SKIP {order_id}: no street address for yardi_id={yardi_id}")
                skipped += 1
                continue

            utilities = parse_utilities(utilities_str)
            is_urgent = requested_for is None

            # Build expected title and description
            util_abbrev = "".join(u[0] for u in utilities)
            reason_display = REASON_DISPLAY.get(reason, reason)
            expected_title = f"[{street}] {reason_display} ({util_abbrev})"
            expected_description = build_linear_description(
                order_id, yardi_id, utilities, reason,
                is_urgent, requested_at or "", requested_for, special_instructions
            )
            expected_priority = 1 if is_urgent else 0
            expected_due_date = requested_for

            # Fetch current Linear issue
            issue = fetch_linear_issue(client, linear_id)
            if not issue:
                print(f"ERROR {order_id}: could not fetch Linear issue {linear_id}")
                skipped += 1
                continue

            identifier = issue.get("identifier", "?")
            current_title = issue.get("title", "")
            current_description = issue.get("description", "")
            current_priority = issue.get("priority", 0)
            current_due_date = issue.get("dueDate")

            # Check for discrepancies
            diffs = []
            if current_title != expected_title:
                diffs.append(("title", current_title, expected_title))
            if current_description.strip() != expected_description.strip():
                diffs.append(("description", current_description.strip(), expected_description.strip()))
            if current_priority != expected_priority:
                diffs.append(("priority", current_priority, expected_priority))
            if current_due_date != expected_due_date:
                diffs.append(("dueDate", current_due_date, expected_due_date))

            if not diffs:
                continue  # No changes needed

            # Show discrepancies
            print(f"=== {identifier} ===")
            for field, current, expected in diffs:
                if field == "description":
                    print(f"  {field}:")
                    print(f"    CURRENT:\n{current}")
                    print(f"    EXPECTED:\n{expected}")
                else:
                    print(f"  {field}: {current!r} -> {expected!r}")

            # Prompt
            answer = input("Update? [y/n/q] ").strip().lower()
            if answer == 'q':
                print("Quitting.")
                break
            if answer != 'y':
                skipped += 1
                print()
                continue

            # Update
            variables = {
                "id": linear_id,
                "input": {
                    "title": expected_title,
                    "description": expected_description,
                    "priority": expected_priority,
                    "dueDate": expected_due_date,
                }
            }

            response = client.post(
                LINEAR_API_URL,
                json={"query": UPDATE_MUTATION, "variables": variables},
                headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            )

            result = response.json()
            if "errors" in result:
                error = result["errors"][0].get("message", "Unknown error")
                print(f"  ERROR: {error}")
            else:
                print(f"  Updated.")
                updated += 1
            print()

    cur.close()
    conn.close()
    print(f"Done! Updated: {updated}, Skipped: {skipped}")


if __name__ == "__main__":
    sync()
