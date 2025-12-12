#!/usr/bin/env python3
import os
from datetime import datetime, timezone
import hmac
import hashlib
import httpx
import psycopg2
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

from enums import Utility, Reason, REASON_DISPLAY, UTILITY_ABBREV_MAP, get_suborder_status

load_dotenv(os.path.expanduser("~/.env"), override=False)
DATABASE_URL = os.environ.get("DATABASE_URL")
LINEAR_API_KEY = os.environ.get("LINEAR_API_KEY")
LINEAR_API_URL = "https://api.linear.app/graphql"

LINEAR_PROJECT_ID = "fecbb569-44a0-4985-ab89-a564be22bc91"
LINEAR_TEAM_ID = "cf213fca-23a7-49b8-99c6-f7d5fb436b87"
LINEAR_TODO_STATE_ID = "6b5ac552-9d79-413c-9adc-3e50faffad41"
LINEAR_BACKLOG_STATE_ID = "d90c0f07-fdf7-43b5-82e5-76735bd6464f"
SUBORDERS_PROJECT_ID = "d5abf424-7d87-450b-b722-d08dc67a7105"
LINEAR_WEBHOOK_SECRET = "lin_wh_6UVVgcR1GDrE6AeXzv58EZoE8hqUAecgXaHzIV2O7SVG"


def generate_order_id() -> str:
    """Generate order ID in format YYYYMMDD-XXX where XXX is a sequential number for the day."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM portal.\"order\" WHERE id LIKE %s ORDER BY id DESC LIMIT 1",
                (f"{today}-%",)
            )
            row = cur.fetchone()
            if row:
                last_seq = int(row[0].split("-")[1])
                next_seq = last_seq + 1
            else:
                next_seq = 1
            return f"{today}-{next_seq:03d}"


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_property_street(code: str) -> str | None:
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT address__addr1 FROM propify.property WHERE foreign_db_code = %s",
                (code,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def get_property_details(code: str) -> dict | None:
    """Get full property details from propify.property"""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    foreign_db_code,
                    address__addr1,
                    address__city,
                    address__state,
                    address__postal_code,
                    address__latitude,
                    address__longitude,
                    county,
                    holding_company_id,
                    transaction_status,
                    type,
                    year_built,
                    acquisition_date,
                    unit_status
                FROM propify.property
                WHERE foreign_db_code = %s
            """, (code,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "propify_id": row[0],
                "code": row[1],
                "street": row[2],
                "city": row[3],
                "state": row[4],
                "zip": row[5],
                "lat": row[6],
                "lng": row[7],
                "county": row[8],
                "holding_company_id": row[9],
                "status": row[10],
                "type": row[11],
                "year_built": row[12],
                "acquisition_date": str(row[13]) if row[13] else None,
                "occupancy": row[14],
            }


def get_property_utilities(code: str) -> list[dict]:
    """Get utility records from propify.utility"""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    type,
                    vendor_name,
                    vendor_contact,
                    account_name,
                    account_number,
                    responsible_party_role_type,
                    status,
                    active,
                    account_start_date,
                    account_stop_date
                FROM propify.utility
                WHERE foreign_db_code = %s
                ORDER BY account_start_date DESC NULLS LAST
            """, (code,))
            rows = cur.fetchall()
            return [
                {
                    "type": row[0],
                    "vendor_name": row[1] or "",
                    "vendor_contact": row[2] or "",
                    "account_name": row[3] or "",
                    "account_number": row[4] or "",
                    "responsible_party": row[5] or "",
                    "status": row[6] or "",
                    "active": row[7],
                    "start_date": str(row[8]) if row[8] else "",
                    "stop_date": str(row[9]) if row[9] else "",
                }
                for row in rows
            ]


def build_utilities_table(utilities: list[dict]) -> str:
    """Build markdown tables for utilities, grouped by type"""
    if not utilities:
        return ""

    electric = [u for u in utilities if u["type"] == "ELECTRICITY"]
    gas = [u for u in utilities if u["type"] == "GAS"]
    other = [u for u in utilities if u["type"] not in ["ELECTRICITY", "GAS"]]

    output = ""
    header = "| Type | Vendor | Vendor Contact | Account Name | Account # | Responsible Party | Status | Active | Start Date | Stop Date |\n"
    divider = "|------|--------|----------------|--------------|-----------|-------------------|--------|--------|------------|----------|\n"

    def make_row(u: dict) -> str:
        return f"| {u['type']} | {u['vendor_name']} | {u['vendor_contact']} | {u['account_name']} | {u['account_number']} | {u['responsible_party']} | {u['status']} | {u['active']} | {u['start_date']} | {u['stop_date']} |\n"

    if electric:
        output += "\n\n**Electric**\n\n" + header + divider
        for u in electric:
            output += make_row(u)

    if gas:
        output += "\n\n**Gas**\n\n" + header + divider
        for u in gas:
            output += make_row(u)

    if other:
        output += "\n\n**Water / Sewer / Trash**\n\n" + header + divider
        for u in other:
            output += make_row(u)

    return output


def build_linear_description(prop: dict, utilities: list[dict]) -> str:
    """Build Linear issue description with property info and utilities"""
    address = f"{prop['street']}, {prop['city']}, {prop['state']} {prop['zip']}"
    location = f"{prop['lat']}, {prop['lng']}" if prop['lat'] and prop['lng'] else "N/A"
    propify_url = f"https://admin.propify.com/properties/{prop['propify_id']}"

    lines = [
        "### Property\n",
        f"- **Propify URL**: {propify_url}",
        f"- **Address**: {address}",
        f"- **Location**: {location}",
        f"- **County**: {prop['county'] or 'N/A'}",
        f"- **Code**: {prop['code']}",
        f"- **Status**: {prop['status'] or 'N/A'}",
        f"- **Type**: {prop['type'] or 'N/A'}",
        f"- **Year Built**: {prop['year_built'] or 'N/A'}",
        f"- **Acquisition Date**: {prop['acquisition_date'] or 'N/A'}",
        f"- **Occupancy**: {prop['occupancy'] or 'N/A'}",
    ]

    description = "\n".join(lines)

    if utilities:
        description += "\n\n### Utilities" + build_utilities_table(utilities)

    return description


def build_order_metadata_comment(order_id: str, yardi_id: str, utilities: list[str], reason: str,
                                  requested_at: str, requested_for: str | None,
                                  special_instructions: str | None) -> str:
    """Build Order Metadata comment for Linear issue"""
    return f"""+++ **Order Metadata**

```
order_id: {order_id}
yardi_id: {yardi_id}
requested_at: {requested_at}
utilities: [{", ".join(utilities)}]
requested_for: {requested_for or "ASAP"}
reason: {reason}
special_instructions: {special_instructions or "null"}
```

+++"""


def build_suborder_data_block() -> str:
    """Build Suborder Data block for Linear issue description (all fields empty at creation)"""
    return """+++ **Suborder Data**

```
scheduled_for:
account_number:
deposit_paid:
deposit_amount:
deposit_confirmation_number:
```

+++"""


class OrderCreate(BaseModel):
    code: str
    utilities: list[Utility]
    reason: Reason
    requested_for: str | None = None  # null = urgent, non-null = scheduled
    special_instructions: str | None = None

    @field_validator("utilities")
    @classmethod
    def utilities_non_empty(cls, v):
        if not v:
            raise ValueError("utilities must have at least one item")
        return v


class OrderUpdate(BaseModel):
    code: str
    utilities: list[Utility]
    reason: Reason
    requested_for: str | None = None
    special_instructions: str | None = None

    @field_validator("utilities")
    @classmethod
    def utilities_non_empty(cls, v):
        if not v:
            raise ValueError("utilities must have at least one item")
        return v


class OrderCreateResponse(BaseModel):
    linear_id: str | None = None
    error: str | None = None


class PropertyResponse(BaseModel):
    id: str
    code: str
    address: str
    city: str
    state: str
    zip: str
    venture: str | None


class SuborderResponse(BaseModel):
    linear_id: str
    order_linear_id: str
    utilities: list[str]
    provider: str | None
    scheduled_for: str | None
    status: str


class OrderResponse(BaseModel):
    id: str
    reason: str
    yardi_id: str
    street: str | None
    city: str | None
    state: str | None
    utilities: list[str]
    requested_at: str | None
    requested_for: str | None
    special_instructions: str | None
    status: str
    completed_on: str | None
    suborders: list[SuborderResponse]


def parse_utilities(utilities_str: str) -> list[str]:
    """Parse utilities from '[ELECTRIC, GAS]' format to list"""
    if not utilities_str:
        return []
    cleaned = utilities_str.strip("[]")
    if not cleaned:
        return []
    return [u.strip() for u in cleaned.split(",")]


@app.get("/orders", response_model=list[OrderResponse])
def get_orders():
    """Get all orders from portal.order (source of truth)"""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    o.id,
                    o.reason,
                    o.yardi_id,
                    p.address__addr1 as street,
                    p.address__city as city,
                    p.address__state as state,
                    o.utilities,
                    o.requested_at,
                    o.requested_for,
                    o.special_instructions,
                    o.status,
                    o.completed_on,
                    o.linear_id
                FROM portal."order" o
                LEFT JOIN propify.property p ON o.yardi_id = p.foreign_db_code
                ORDER BY o.requested_at DESC
            """)
            order_rows = cur.fetchall()

            # Suborders come from portal.suborder (synced from Linear webhook)
            # order_linear_id links to portal.order.linear_id
            cur.execute("""
                SELECT
                    s.linear_id,
                    s.order_linear_id,
                    s.utilities,
                    s.provider,
                    s.scheduled_for,
                    s.status
                FROM portal.suborder s
            """)
            suborder_rows = cur.fetchall()

    # Build suborders lookup by order_linear_id (links to portal.order.linear_id)
    suborders_by_order_linear_id = {}
    for row in suborder_rows:
        order_linear_id = str(row[1])
        suborder = SuborderResponse(
            linear_id=row[0],
            order_linear_id=order_linear_id,
            utilities=parse_utilities(row[2]),
            provider=row[3],
            scheduled_for=str(row[4]) if row[4] else None,
            status=row[5] or "TODO",
        )
        if order_linear_id not in suborders_by_order_linear_id:
            suborders_by_order_linear_id[order_linear_id] = []
        suborders_by_order_linear_id[order_linear_id].append(suborder)

    orders = []
    for row in order_rows:
        order_id = str(row[0])
        order_linear_id = str(row[12]) if row[12] else None
        orders.append(OrderResponse(
            id=order_id,
            reason=row[1],
            yardi_id=row[2],
            street=row[3],
            city=row[4],
            state=row[5],
            utilities=parse_utilities(row[6]),
            requested_at=row[7].isoformat() if row[7] else None,
            requested_for=str(row[8]) if row[8] else None,
            special_instructions=row[9],
            status=row[10],
            completed_on=str(row[11]) if row[11] else None,
            suborders=suborders_by_order_linear_id.get(order_linear_id, []) if order_linear_id else [],
        ))

    return orders


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.get("/properties", response_model=list[PropertyResponse])
def get_properties():
    """Get all properties from propify.property with venture name"""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.id,
                    p.foreign_db_code,
                    p.address__addr1,
                    p.address__city,
                    p.address__state,
                    p.address__postal_code,
                    v.name as venture_name
                FROM propify.property p
                LEFT JOIN propify.venture v ON p.venture_id = v.id
                ORDER BY p.foreign_db_code
            """)
            rows = cur.fetchall()

    return [
        PropertyResponse(
            id=str(row[0]),
            code=row[1] or "",
            address=row[2] or "",
            city=row[3] or "",
            state=row[4] or "",
            zip=row[5] or "",
            venture=row[6],
        )
        for row in rows
    ]


@app.post("/orders", response_model=OrderCreateResponse)
async def create_order(order: OrderCreate):
    """Create order: insert into portal.order, create Linear issue"""
    CREATE_ISSUE_MUTATION = """
        mutation CreateIssue($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue {
                    id
                    identifier
                    url
                }
            }
        }
    """

    CREATE_COMMENT_MUTATION = """
        mutation CreateComment($input: CommentCreateInput!) {
            commentCreate(input: $input) {
                success
                comment { id }
            }
        }
    """

    # Get property details and utilities from Propify
    prop = get_property_details(order.code)
    if not prop:
        return OrderCreateResponse(error=f"Property not found: {order.code}")
    street = prop["street"]
    prop_utilities = get_property_utilities(order.code)

    order_id = generate_order_id()
    requested_at = datetime.now(timezone.utc)
    is_urgent = order.requested_for is None
    utilities_str = f"[{', '.join(order.utilities)}]"
    special_instructions = order.special_instructions.strip() if order.special_instructions else None

    # Build Linear issue
    util_abbrev = "".join(u[0] for u in order.utilities)
    title = f"[{street}] {REASON_DISPLAY[order.reason]} ({util_abbrev})"
    description = build_linear_description(prop, prop_utilities)
    order_metadata_comment = build_order_metadata_comment(
        order_id, order.code, list(order.utilities), order.reason,
        requested_at.strftime("%Y-%m-%dT%H:%M:%SZ"), order.requested_for, special_instructions
    )

    variables = {
        "input": {
            "teamId": LINEAR_TEAM_ID,
            "projectId": LINEAR_PROJECT_ID,
            "stateId": LINEAR_TODO_STATE_ID,
            "title": title,
            "description": description,
            "priority": 1 if is_urgent else 0,
        }
    }
    if order.requested_for:
        variables["input"]["dueDate"] = order.requested_for

    # Create Linear issue
    async with httpx.AsyncClient() as client:
        response = await client.post(
            LINEAR_API_URL,
            json={"query": CREATE_ISSUE_MUTATION, "variables": variables},
            headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
        )

        result = response.json()
        if "errors" in result:
            return OrderCreateResponse(error=result["errors"][0].get("message", "Unknown error"))

        issue = result.get("data", {}).get("issueCreate", {}).get("issue", {})
        linear_id = issue.get("id")  # UUID for API calls

        # Add Portal Data as a comment
        comment_response = await client.post(
            LINEAR_API_URL,
            json={
                "query": CREATE_COMMENT_MUTATION,
                "variables": {"input": {"issueId": linear_id, "body": order_metadata_comment}}
            },
            headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
        )
        # We don't fail if comment creation fails - the issue is still created

        # Create suborders (one per utility)
        utility_abbrevs = {"ELECTRIC": "E", "GAS": "G", "WATER": "W"}
        for utility in order.utilities:
            abbrev = utility_abbrevs.get(utility, utility[0])
            suborder_title = f"Activate {abbrev} via ?"
            suborder_description = build_suborder_data_block()

            suborder_vars = {
                "input": {
                    "teamId": LINEAR_TEAM_ID,
                    "projectId": SUBORDERS_PROJECT_ID,
                    "stateId": LINEAR_BACKLOG_STATE_ID,
                    "parentId": linear_id,
                    "title": suborder_title,
                    "description": suborder_description,
                    "priority": 1 if is_urgent else 0,
                    "labelIds": ["42873d74-0557-4e31-a432-fb3fc67f44e5"],  # "ID" label
                }
            }
            if order.requested_for:
                suborder_vars["input"]["dueDate"] = order.requested_for

            suborder_response = await client.post(
                LINEAR_API_URL,
                json={"query": CREATE_ISSUE_MUTATION, "variables": suborder_vars},
                headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            )

            # Insert suborder into portal.suborder immediately
            suborder_result = suborder_response.json()
            suborder_issue = suborder_result.get("data", {}).get("issueCreate", {}).get("issue", {})
            suborder_linear_id = suborder_issue.get("id")
            if suborder_linear_id:
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO portal.suborder (linear_id, order_linear_id, utilities, provider, scheduled_for, status)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (linear_id) DO NOTHING
                        """, (
                            suborder_linear_id,
                            linear_id,
                            f"[{utility}]",
                            "?",
                            order.requested_for,
                            "BACKLOG"
                        ))
                    conn.commit()

    # Insert into portal.order (source of truth)
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO portal."order" (
                    id, linear_id, yardi_id, utilities, reason,
                    requested_at, requested_for, special_instructions, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_id, linear_id, order.code, utilities_str,
                order.reason, requested_at, order.requested_for, special_instructions, "TODO"
            ))
        conn.commit()

    return OrderCreateResponse()


@app.patch("/orders/{order_id}", response_model=OrderCreateResponse)
async def update_order(order_id: str, order: OrderUpdate):
    """Update order: update portal.order, update Linear issue"""
    UPDATE_MUTATION = """
        mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
                issue { id identifier }
            }
        }
    """

    FETCH_COMMENTS_QUERY = """
        query GetIssueComments($issueId: String!) {
            issue(id: $issueId) {
                comments {
                    nodes {
                        id
                        body
                    }
                }
            }
        }
    """

    UPDATE_COMMENT_MUTATION = """
        mutation UpdateComment($id: String!, $input: CommentUpdateInput!) {
            commentUpdate(id: $id, input: $input) {
                success
                comment { id }
            }
        }
    """

    CREATE_COMMENT_MUTATION = """
        mutation CreateComment($input: CommentCreateInput!) {
            commentCreate(input: $input) {
                success
                comment { id }
            }
        }
    """

    # Validate property exists
    street = get_property_street(order.code)
    if not street:
        return OrderCreateResponse(error=f"Property not found: {order.code}")

    # Get order from portal.order (with old values for change tracking)
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linear_id, requested_at, yardi_id, utilities, reason, requested_for, special_instructions
                FROM portal."order" WHERE id = %s
            """, (order_id,))
            row = cur.fetchone()
            if not row:
                return OrderCreateResponse(error=f"Order not found: {order_id}")
            linear_id = str(row[0]) if row[0] else None
            requested_at = row[1].strftime("%Y-%m-%dT%H:%M:%SZ") if row[1] else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            # Old values for change tracking
            old_yardi_id = row[2]
            old_utilities = row[3]  # stored as "[ELECTRIC, GAS]"
            old_reason = row[4]
            old_requested_for = str(row[5]) if row[5] else None
            old_special_instructions = row[6]

    if not linear_id:
        return OrderCreateResponse(error=f"Linear issue ID not found for order: {order_id}")

    # Build updated values
    is_urgent = order.requested_for is None
    utilities_str = f"[{', '.join(order.utilities)}]"
    special_instructions = order.special_instructions.strip() if order.special_instructions else None

    # Update Linear issue
    util_abbrev = "".join(u[0] for u in order.utilities)
    title = f"[{street}] {REASON_DISPLAY[order.reason]} ({util_abbrev})"
    order_metadata_comment = build_order_metadata_comment(
        order_id, order.code, list(order.utilities), order.reason,
        requested_at, order.requested_for, special_instructions
    )

    variables = {
        "id": linear_id,
        "input": {
            "title": title,
            "priority": 1 if is_urgent else 0,
            "dueDate": order.requested_for,
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            LINEAR_API_URL,
            json={"query": UPDATE_MUTATION, "variables": variables},
            headers={"Authorization": LINEAR_API_KEY},
        )

        result = response.json()
        if "errors" in result:
            return OrderCreateResponse(error=result["errors"][0].get("message", "Unknown error"))

        # Find existing Portal Data comment
        comments_response = await client.post(
            LINEAR_API_URL,
            json={"query": FETCH_COMMENTS_QUERY, "variables": {"issueId": linear_id}},
            headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
        )
        comments_result = comments_response.json()
        comments = comments_result.get("data", {}).get("issue", {}).get("comments", {}).get("nodes", [])

        # Find comment containing "Order Metadata" (or legacy "Portal Data")
        metadata_comment_id = None
        for comment in comments:
            body = comment.get("body", "")
            if "Order Metadata" in body or "Portal Data" in body:
                metadata_comment_id = comment.get("id")
                break

        if metadata_comment_id:
            # Update existing comment
            await client.post(
                LINEAR_API_URL,
                json={
                    "query": UPDATE_COMMENT_MUTATION,
                    "variables": {"id": metadata_comment_id, "input": {"body": order_metadata_comment}}
                },
                headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            )
        else:
            # No existing metadata comment, create new one
            await client.post(
                LINEAR_API_URL,
                json={
                    "query": CREATE_COMMENT_MUTATION,
                    "variables": {"input": {"issueId": linear_id, "body": order_metadata_comment}}
                },
                headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            )

        # Build change comment if any fields changed
        changes = []
        new_utilities_str = f"[{', '.join(order.utilities)}]"
        new_requested_for = order.requested_for
        new_special_instructions = order.special_instructions.strip() if order.special_instructions else None

        if old_yardi_id != order.code:
            changes.append(f"Yardi ID: {old_yardi_id} -> {order.code}")
        if old_utilities != new_utilities_str:
            changes.append(f"Utilities: {old_utilities} -> {new_utilities_str}")
        if old_requested_for != new_requested_for:
            old_display = old_requested_for or "ASAP"
            new_display = new_requested_for or "ASAP"
            changes.append(f"Requested for: {old_display} -> {new_display}")
        if old_reason != order.reason:
            changes.append(f"Reason: {old_reason} -> {order.reason}")
        if old_special_instructions != new_special_instructions:
            old_instr = old_special_instructions or "null"
            new_instr = new_special_instructions or "null"
            changes.append(f"Special instructions: {old_instr} -> {new_instr}")

        if changes:
            change_comment = "Changed via portal:\n\n```\n" + "\n".join(changes) + "\n```"
            await client.post(
                LINEAR_API_URL,
                json={
                    "query": CREATE_COMMENT_MUTATION,
                    "variables": {"input": {"issueId": linear_id, "body": change_comment}}
                },
                headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            )

    # Update portal.order (source of truth)
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE portal."order"
                SET yardi_id = %s, utilities = %s, reason = %s, requested_for = %s,
                    special_instructions = %s, updated_at = NOW()
                WHERE id = %s
            """, (order.code, utilities_str, order.reason, order.requested_for,
                  special_instructions, order_id))
        conn.commit()

    update_result = result.get("data", {}).get("issueUpdate", {}).get("issue", {})
    return OrderCreateResponse(linear_id=update_result.get("identifier"))


# Linear "Canceled" state ID for the NHR team
LINEAR_CANCELED_STATE_ID = "9971dd87-29c5-4039-bccb-bdd09082299a"


@app.delete("/orders/{order_id}", response_model=OrderCreateResponse)
async def cancel_order(order_id: str):
    """Cancel order: set status to CANCELLED in portal.order and Linear"""
    CANCEL_MUTATION = """
        mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
                issue { id identifier }
            }
        }
    """

    # Get order from portal.order
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linear_id FROM portal."order" WHERE id = %s
            """, (order_id,))
            row = cur.fetchone()
            if not row:
                return OrderCreateResponse(error=f"Order not found: {order_id}")
            linear_id = str(row[0]) if row[0] else None

    # Update Linear issue to Canceled state
    if linear_id:
        variables = {
            "id": linear_id,
            "input": {
                "stateId": LINEAR_CANCELED_STATE_ID,
            }
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                LINEAR_API_URL,
                json={"query": CANCEL_MUTATION, "variables": variables},
                headers={"Authorization": LINEAR_API_KEY},
            )

        result = response.json()
        if "errors" in result:
            return OrderCreateResponse(error=result["errors"][0].get("message", "Unknown error"))

    # Update portal.order status to CANCELLED
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE portal."order"
                SET status = 'CANCELED', updated_at = NOW()
                WHERE id = %s
            """, (order_id,))
        conn.commit()

    return OrderCreateResponse(linear_id=None)


@app.post("/orders/{order_id}/uncancel", response_model=OrderCreateResponse)
async def uncancel_order(order_id: str):
    """Uncancel order: set status back to READY in portal.order and Linear"""
    UNCANCEL_MUTATION = """
        mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
                issue { id identifier }
            }
        }
    """

    # Get order from portal.order
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linear_id FROM portal."order" WHERE id = %s
            """, (order_id,))
            row = cur.fetchone()
            if not row:
                return OrderCreateResponse(error=f"Order not found: {order_id}")
            linear_id = str(row[0]) if row[0] else None

    # Update Linear issue back to Todo state
    if linear_id:
        variables = {
            "id": linear_id,
            "input": {
                "stateId": LINEAR_TODO_STATE_ID,
            }
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                LINEAR_API_URL,
                json={"query": UNCANCEL_MUTATION, "variables": variables},
                headers={"Authorization": LINEAR_API_KEY},
            )

        result = response.json()
        if "errors" in result:
            return OrderCreateResponse(error=result["errors"][0].get("message", "Unknown error"))

    # Update portal.order status back to READY
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE portal."order"
                SET status = 'TODO', updated_at = NOW()
                WHERE id = %s
            """, (order_id,))
        conn.commit()

    return OrderCreateResponse(linear_id=None)


import re


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


@app.post("/webhooks/linear")
async def linear_webhook(request: Request):
    """Handle Linear webhook events for suborder updates.

    Extracts data from:
    - order_id: Look up portal.order by parent issue's linear_id
    - utilities/provider: Parse from title "Activate [EG] via [Provider]"
    - status: Derive from state + labels (terminal states take precedence)
    - scheduled_for: Parse from description "scheduled_for: 2025-12-15"
    """
    body = await request.body()
    signature = request.headers.get("linear-signature", "")

    # Log incoming webhook for debugging
    print(f"[WEBHOOK] Received webhook, signature present: {bool(signature)}")

    # # Verify signature (DISABLED FOR DEVELOPMENT)
    # expected = hmac.new(LINEAR_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    # if not hmac.compare_digest(expected, signature):
    #     print(f"[WEBHOOK] Signature mismatch! Expected: {expected[:20]}..., Got: {signature[:20] if signature else 'none'}...")
    #     raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await request.json()
    event_type = payload.get("type")
    action = payload.get("action")
    data = payload.get("data", {})

    # Only handle Issue events for suborders project
    if event_type != "Issue":
        return {"status": "ignored", "reason": "not an Issue event"}

    project_id = data.get("projectId")
    if project_id != SUBORDERS_PROJECT_ID:
        return {"status": "ignored", "reason": "not a suborder issue"}

    linear_id = data.get("id")
    linear_identifier = data.get("identifier")

    print(f"[WEBHOOK] action={action}, identifier={linear_identifier}, linear_id={linear_id}")

    # Handle deletes
    if action == "remove":
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM portal.suborder WHERE linear_id = %s", (linear_id,))
            conn.commit()
        print(f"[WEBHOOK] Deleted suborder {linear_identifier}")
        return {"status": "ok", "action": "deleted", "linear_id": linear_identifier}

    # Get parent order (suborders are sub-issues of orders)
    parent = data.get("parent", {})
    order_linear_id = parent.get("id") if parent else None

    # For updates, Linear might not include parent - look it up from DB
    if not order_linear_id:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT order_linear_id FROM portal.suborder WHERE linear_id = %s", (linear_id,))
                row = cur.fetchone()
                if row:
                    order_linear_id = row[0]
                    print(f"[WEBHOOK] Looked up order_linear_id from DB: {order_linear_id}")

    if not order_linear_id:
        print(f"[WEBHOOK] Ignoring {linear_identifier}: no parent issue and not found in DB")
        return {"status": "ignored", "reason": "no parent issue"}

    # Parse title for utilities/provider: "Activate EG via Xcel Energy"
    title = data.get("title", "")
    parsed = parse_suborder_title(title)
    if not parsed:
        return {"status": "ignored", "reason": "title format invalid - expected 'Activate EGW via Provider'"}
    utilities, provider = parsed

    # Get state and labels for status
    state = data.get("state", {})
    state_name = state.get("name", "Todo") if state else "Todo"
    labels = data.get("labels", [])
    label_names = [l.get("name", "") for l in labels] if labels else []
    status = get_suborder_status(state_name, label_names)

    # Parse scheduled_for from description (simple line, not in Portal Data block)
    description = data.get("description", "")
    scheduled_for = parse_scheduled_for(description)

    # Format utilities for storage
    utilities_str = f"[{', '.join(utilities)}]"

    # Upsert suborder (no 'id' column - using linear_id as PK)
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
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
        conn.commit()

    print(f"[WEBHOOK] Upserted suborder {linear_identifier}: provider={provider}, status={status}")
    return {"status": "ok", "action": action, "linear_id": linear_id, "order_linear_id": order_linear_id}
