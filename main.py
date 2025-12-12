#!/usr/bin/env python3
import os
import uuid
from datetime import date
from typing import Literal

import httpx
import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)
DATABASE_URL = os.environ.get("DATABASE_URL")
LINEAR_API_KEY = os.environ.get("LINEAR_API_KEY")
LINEAR_API_URL = "https://api.linear.app/graphql"

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


class PortalData:
    def __init__(
        self,
        type: str,
        id: str,
        requested_on: str,
        yardi_id: str,
        utilities: list[str],
        reason: str,
        is_urgent: bool,
        requested_for: str | None,
        special_instructions: str | None,
    ):
        self.type = type
        self.id = id
        self.requested_on = requested_on
        self.yardi_id = yardi_id
        self.utilities = utilities
        self.reason = reason
        self.is_urgent = is_urgent
        self.requested_for = requested_for
        self.special_instructions = special_instructions

    def to_description(self) -> str:
        return f"""+++ **Portal Data**

```
type: {self.type}
id: {self.id}
requested_on: {self.requested_on}
yardi_id: {self.yardi_id}
utilities: [{", ".join(self.utilities)}]
reason: {self.reason}
is_urgent: {str(self.is_urgent).lower()}
requested_for: {self.requested_for or "null"}
special_instructions: {self.special_instructions or "null"}
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


class OrderCreateResponse(BaseModel):
    linear_id: str | None = None
    error: str | None = None


class OrderResponse(BaseModel):
    id: str
    reason: str
    yardi_id: str
    street: str | None
    state: str | None
    utilities: list[str]
    requested_on: str | None
    requested_for: str | None
    special_instructions: str | None
    status: str
    completed_on: str | None


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
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    o.data->>'id' as id,
                    o.data->>'reason' as reason,
                    o.data->>'yardi_id' as yardi_id,
                    p.address__addr1 as street,
                    p.address__state as state,
                    o.data->>'utilities' as utilities,
                    o.data->>'requested_on' as requested_on,
                    o.data->>'requested_for' as requested_for,
                    o.data->>'special_instructions' as special_instructions
                FROM portal."order" o
                LEFT JOIN propify.property p ON o.data->>'yardi_id' = p.foreign_db_code
            """)
            rows = cur.fetchall()

    orders = []
    for row in rows:
        orders.append(OrderResponse(
            id=row[0],
            reason=row[1],
            yardi_id=row[2],
            street=row[3],
            state=row[4],
            utilities=parse_utilities(row[5]),
            requested_on=row[6],
            requested_for=row[7] if row[7] != "null" else None,
            special_instructions=row[8] if row[8] != "null" else None,
            status="READY",
            completed_on=None,
        ))
    return orders


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.post("/orders", response_model=OrderCreateResponse)
async def create_order(order: OrderCreate):
    LINEAR_PROJECT_ID = "fecbb569-44a0-4985-ab89-a564be22bc91"
    LINEAR_TEAM_ID = "cf213fca-23a7-49b8-99c6-f7d5fb436b87"
    LINEAR_TODO_STATE_ID = "6b5ac552-9d79-413c-9adc-3e50faffad41"
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
    util_abbrev = "".join(u[0] for u in order.utilities)
    title = f"[{street}] {REASON_DISPLAY[order.reason]} ({util_abbrev})"

    portal_data = PortalData(
        type="Order",
        id=order_id,
        requested_on=requested_on,
        yardi_id=order.code,
        utilities=list(order.utilities),
        reason=order.reason,
        is_urgent=is_urgent,
        requested_for=order.requested_for,
        special_instructions=order.special_instructions.strip() if order.special_instructions else None,
    )

    linear_priority = 1 if is_urgent else 0
    variables = {
        "input": {
            "teamId": LINEAR_TEAM_ID,
            "projectId": LINEAR_PROJECT_ID,
            "stateId": LINEAR_TODO_STATE_ID,
            "title": title,
            "description": portal_data.to_description(),
            "priority": linear_priority,
        }
    }

    if order.requested_for:
        variables["input"]["dueDate"] = order.requested_for

    async with httpx.AsyncClient() as client:
        response = await client.post(
            LINEAR_API_URL,
            json={"query": CREATE_ISSUE_MUTATION, "variables": variables},
            headers={
                "Content-Type": "application/json",
                "Authorization": LINEAR_API_KEY,
            },
        )

    result = response.json()

    if "errors" in result:
        return OrderCreateResponse(
            error=result["errors"][0].get("message", "Unknown error"),
        )

    issue = result.get("data", {}).get("issueCreate", {}).get("issue", {})
    return OrderCreateResponse(
        linear_id=issue.get("identifier"),
    )
