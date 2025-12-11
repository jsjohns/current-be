#!/usr/bin/env python3
import os
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

app = FastAPI()

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LINEAR_API_KEY = os.environ.get("LINEAR_API_KEY")
LINEAR_API_URL = "https://api.linear.app/graphql"
LINEAR_PROJECT_ID = "fecbb569-44a0-4985-ab89-a564be22bc91"
LINEAR_TEAM_ID = "cf213fca-23a7-49b8-99c6-f7d5fb436b87"


from pydantic import field_validator, model_validator
from typing import Literal

VALID_UTILITIES = ["Electric", "Gas", "Water", "Sewer", "Trash"]
Utility = Literal["Electric", "Gas", "Water", "Sewer", "Trash"]


class OrderCreate(BaseModel):
    code: str
    utilities: list[Utility]
    reason: str
    is_urgent: bool = False
    requested_for: str | None = None
    requested_on: str
    special_instructions: str | None = None

    @field_validator("utilities")
    @classmethod
    def utilities_non_empty(cls, v):
        if not v:
            raise ValueError("utilities must have at least one item")
        return v

    @model_validator(mode="after")
    def check_requested_for(self):
        if self.is_urgent and self.requested_for is not None:
            raise ValueError("requested_for must be null when is_urgent is true")
        if not self.is_urgent and self.requested_for is None:
            raise ValueError("requested_for is required when is_urgent is false")
        return self


class OrderCreateResponse(BaseModel):
    success: bool
    identifier: str | None = None
    error: str | None = None


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.post("/orders", response_model=OrderCreateResponse)
async def create_order(order: OrderCreate):
    if not LINEAR_API_KEY:
        raise HTTPException(status_code=500, detail="Server configuration error")

    import uuid

    # Generate order ID
    order_id = str(uuid.uuid4())

    # TODO: Look up property by code to get address, city, state
    address = "907 St. Mary's Street"
    city = "Raleigh"
    state = "NC"

    # Format utilities abbreviation (E, G, W, T)
    util_abbrev = "".join(u[0] for u in order.utilities)

    # Format title: "[$street, $city, $state] $reason ($utilities)"
    if address:
        title = f"[{address}, {city}, {state}] {order.reason} ({util_abbrev})"
    else:
        title = f"[{order.code}] {order.reason} ({util_abbrev})"

    # Format description with portal fields
    special_instructions = order.special_instructions.strip() if order.special_instructions else None

    description = f"""+++ **Portal Fields**

```
type: Order
id: {order_id}
requested_on: {order.requested_on}
yardi_id: {order.code}
utilities: [{", ".join(order.utilities)}]
reason: {order.reason}
is_urgent: {str(order.is_urgent).lower()}
requested_for: {order.requested_for or "null"}
special_instructions: {special_instructions or "null"}
```

+++"""

    # Map priority
    linear_priority = 1 if order.is_urgent else 0

    mutation = """
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

    variables = {
        "input": {
            "teamId": LINEAR_TEAM_ID,
            "projectId": LINEAR_PROJECT_ID,
            "title": title,
            "description": description,
            "priority": linear_priority,
        }
    }

    if not order.is_urgent and order.requested_for:
        variables["input"]["dueDate"] = order.requested_for

    async with httpx.AsyncClient() as client:
        response = await client.post(
            LINEAR_API_URL,
            json={"query": mutation, "variables": variables},
            headers={
                "Content-Type": "application/json",
                "Authorization": LINEAR_API_KEY,
            },
        )

    result = response.json()

    if "errors" in result:
        return OrderCreateResponse(
            success=False,
            error=result["errors"][0].get("message", "Unknown error"),
        )

    issue = result.get("data", {}).get("issueCreate", {}).get("issue", {})
    return OrderCreateResponse(
        success=True,
        identifier=issue.get("identifier"),
    )
