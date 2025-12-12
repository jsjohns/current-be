#!/usr/bin/env python3
import os
import uuid
from datetime import date
from typing import Literal

import hmac
import hashlib
import httpx
import psycopg2
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)
DATABASE_URL = os.environ.get("DATABASE_URL")
LINEAR_API_KEY = os.environ.get("LINEAR_API_KEY")
LINEAR_API_URL = "https://api.linear.app/graphql"

LINEAR_PROJECT_ID = "fecbb569-44a0-4985-ab89-a564be22bc91"
LINEAR_TEAM_ID = "cf213fca-23a7-49b8-99c6-f7d5fb436b87"
LINEAR_TODO_STATE_ID = "6b5ac552-9d79-413c-9adc-3e50faffad41"
SUBORDERS_PROJECT_ID = "d5abf424-7d87-450b-b722-d08dc67a7105"
LINEAR_WEBHOOK_SECRET = "lin_wh_6UVVgcR1GDrE6AeXzv58EZoE8hqUAecgXaHzIV2O7SVG"

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


Utility = Literal["ELECTRIC", "GAS", "WATER", "SEWER", "TRASH"]
Reason = Literal["ACQUISITION", "DISPOSITION", "MOVE_OUT", "EVICTION", "ABANDONMENT", "ONBOARDING", "OTHER"]
REASON_DISPLAY = {
    "ACQUISITION": "Acquisition",
    "DISPOSITION": "Disposition",
    "MOVE_OUT": "Move-Out",
    "EVICTION": "Eviction",
    "ABANDONMENT": "Abandonment",
    "ONBOARDING": "Onboarding",
    "OTHER": "Other",
}


def build_linear_description(order_id: str, yardi_id: str, utilities: list[str], reason: str,
                              is_urgent: bool, requested_on: str, requested_for: str | None,
                              special_instructions: str | None) -> str:
    """Build Portal Data description for Linear issue"""
    return f"""+++ **Portal Data**

```
type: Order
id: {order_id}
requested_on: {requested_on}
yardi_id: {yardi_id}
utilities: [{", ".join(utilities)}]
reason: {reason}
is_urgent: {str(is_urgent).lower()}
requested_for: {requested_for or "null"}
special_instructions: {special_instructions or "null"}
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


class SuborderResponse(BaseModel):
    id: str
    order_id: str
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
    requested_on: str | None
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
                    o.requested_on,
                    o.requested_for,
                    o.special_instructions,
                    o.status,
                    o.completed_on
                FROM portal."order" o
                LEFT JOIN propify.property p ON o.yardi_id = p.foreign_db_code
                ORDER BY o.created_at DESC
            """)
            order_rows = cur.fetchall()

            # Suborders come from portal.suborder (synced from Linear)
            cur.execute("""
                SELECT
                    s.id,
                    s.order_id,
                    s.utilities,
                    s.provider,
                    s.scheduled_for,
                    s.status
                FROM portal.suborder s
            """)
            suborder_rows = cur.fetchall()

    # Build suborders lookup by order_id
    suborders_by_order = {}
    for row in suborder_rows:
        order_id = row[1]
        suborder = SuborderResponse(
            id=row[0],
            order_id=order_id,
            utilities=parse_utilities(row[2]),
            provider=row[3],
            scheduled_for=str(row[4]) if row[4] else None,
            status=row[5] or "READY",
        )
        if order_id not in suborders_by_order:
            suborders_by_order[order_id] = []
        suborders_by_order[order_id].append(suborder)

    orders = []
    for row in order_rows:
        order_id = str(row[0])
        orders.append(OrderResponse(
            id=order_id,
            reason=row[1],
            yardi_id=row[2],
            street=row[3],
            city=row[4],
            state=row[5],
            utilities=parse_utilities(row[6]),
            requested_on=str(row[7]) if row[7] else None,
            requested_for=str(row[8]) if row[8] else None,
            special_instructions=row[9],
            status=row[10],
            completed_on=str(row[11]) if row[11] else None,
            suborders=suborders_by_order.get(order_id, []),
        ))

    return orders


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.get("/properties", response_model=list[PropertyResponse])
def get_properties():
    """Get all properties from propify.property"""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    foreign_db_code,
                    address__addr1,
                    address__city,
                    address__state,
                    address__postal_code
                FROM propify.property
                ORDER BY foreign_db_code
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

    street = get_property_street(order.code)
    if not street:
        return OrderCreateResponse(error=f"Property not found: {order.code}")

    order_id = str(uuid.uuid4())
    requested_on = date.today().isoformat()
    is_urgent = order.requested_for is None
    utilities_str = f"[{', '.join(order.utilities)}]"
    special_instructions = order.special_instructions.strip() if order.special_instructions else None

    # Build Linear issue
    util_abbrev = "".join(u[0] for u in order.utilities)
    title = f"[{street}] {REASON_DISPLAY[order.reason]} ({util_abbrev})"
    description = build_linear_description(
        order_id, order.code, list(order.utilities), order.reason,
        is_urgent, requested_on, order.requested_for, special_instructions
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
    linear_identifier = issue.get("identifier")  # "NHR-123"

    # Insert into portal.order (source of truth)
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO portal."order" (
                    id, linear_id, linear_identifier, yardi_id, utilities, reason,
                    requested_on, requested_for, special_instructions, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_id, linear_id, linear_identifier, order.code, utilities_str,
                order.reason, requested_on, order.requested_for, special_instructions, "READY"
            ))
        conn.commit()

    return OrderCreateResponse(linear_id=linear_identifier)


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

    # Validate property exists
    street = get_property_street(order.code)
    if not street:
        return OrderCreateResponse(error=f"Property not found: {order.code}")

    # Get order from portal.order
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linear_id, requested_on FROM portal."order" WHERE id = %s
            """, (order_id,))
            row = cur.fetchone()
            if not row:
                return OrderCreateResponse(error=f"Order not found: {order_id}")
            linear_id = str(row[0]) if row[0] else None
            requested_on = str(row[1]) if row[1] else date.today().isoformat()

    if not linear_id:
        return OrderCreateResponse(error=f"Linear issue ID not found for order: {order_id}")

    # Build updated values
    is_urgent = order.requested_for is None
    utilities_str = f"[{', '.join(order.utilities)}]"
    special_instructions = order.special_instructions.strip() if order.special_instructions else None

    # Update Linear issue
    util_abbrev = "".join(u[0] for u in order.utilities)
    title = f"[{street}] {REASON_DISPLAY[order.reason]} ({util_abbrev})"
    description = build_linear_description(
        order_id, order.code, list(order.utilities), order.reason,
        is_urgent, requested_on, order.requested_for, special_instructions
    )

    variables = {
        "id": linear_id,
        "input": {
            "description": description,
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
                SET status = 'CANCELLED', updated_at = NOW()
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
                SET status = 'READY', updated_at = NOW()
                WHERE id = %s
            """, (order_id,))
        conn.commit()

    return OrderCreateResponse(linear_id=None)


import re


def parse_portal_data(description: str) -> dict | None:
    """Parse Portal Data block from Linear issue description"""
    if not description:
        return None
    match = re.search(r'\*\*Portal Data\*\*\s*\n+```\s*\n([\s\S]*?)\n```', description)
    if not match:
        return None
    content = match.group(1)
    data = {}
    for line in content.split('\n'):
        if ':' in line:
            key = line.split(':', 1)[0].strip()
            value = line.split(':', 1)[1].strip()
            # Handle "null" string as None
            data[key] = None if value == "null" else value
    return data


@app.post("/webhooks/linear")
async def linear_webhook(request: Request):
    """Handle Linear webhook events for suborder updates"""
    body = await request.body()
    signature = request.headers.get("linear-signature", "")

    # Verify signature
    expected = hmac.new(LINEAR_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

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
    description = data.get("description", "")

    if action == "remove":
        # Delete suborder from cache
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM portal.suborder WHERE linear_id = %s", (linear_id,))
            conn.commit()
        return {"status": "ok", "action": "deleted", "linear_id": linear_identifier}

    # Parse Portal Data from description
    portal_data = parse_portal_data(description)
    if not portal_data:
        return {"status": "ignored", "reason": "no Portal Data found"}

    # Upsert suborder
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO portal.suborder (linear_id, linear_identifier, id, order_id, utilities, provider, scheduled_for, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (linear_id) DO UPDATE SET
                    linear_identifier = EXCLUDED.linear_identifier,
                    id = EXCLUDED.id,
                    order_id = EXCLUDED.order_id,
                    utilities = EXCLUDED.utilities,
                    provider = EXCLUDED.provider,
                    scheduled_for = EXCLUDED.scheduled_for,
                    status = EXCLUDED.status
            """, (
                linear_id,
                linear_identifier,
                portal_data.get("id"),
                portal_data.get("order_id"),
                portal_data.get("utilities"),
                portal_data.get("provider"),
                portal_data.get("scheduled_for"),
                portal_data.get("status"),
            ))
        conn.commit()

    return {"status": "ok", "action": action, "linear_id": linear_identifier}
