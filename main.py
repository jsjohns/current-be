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


class OrderCreate(BaseModel):
    id: str
    address: str | None = None
    city: str | None = None
    state: str | None = None
    code: str | None = None  # yardi_id
    utilities: list[str]
    reason: str
    priority: str  # "Normal" or "Urgent"
    target_date: str | None = None
    request_date: str | None = None
    note: str | None = None


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

    # Format utilities abbreviation (E, G, W, T)
    util_abbrev = "".join(u[0] for u in order.utilities)

    # Format title: "[$street, $city, $state] $reason ($utilities)"
    if order.address:
        title = f"[{order.address}, {order.city}, {order.state}] {order.reason} ({util_abbrev})"
    else:
        title = f"[Unknown Property] {order.reason} ({util_abbrev})"

    # Format description with portal fields
    from datetime import date
    today = date.today().isoformat()
    requested_for = "ASAP" if order.priority == "Urgent" else (order.target_date or "N/A")

    description = f"""+++ **Portal Fields**

```
type: Order
id: {order.id}
requested_on: {order.request_date or today}
yardi_id: {order.code or "N/A"}
utilities: {", ".join(order.utilities)}
reason: {order.reason}
requested_for: {requested_for}
special_instructions: {order.note or "N/A"}
```

+++"""

    # Map priority
    linear_priority = 1 if order.priority == "Urgent" else 0

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

    if order.priority != "Urgent" and order.target_date:
        variables["input"]["dueDate"] = order.target_date

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
