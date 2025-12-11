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


class CreateIssueRequest(BaseModel):
    title: str
    description: str
    priority: int = 0  # 0 = No Priority, 1 = Urgent, 2 = High, 3 = Medium, 4 = Low
    due_date: str | None = None


class CreateIssueResponse(BaseModel):
    success: bool
    identifier: str | None = None
    url: str | None = None
    error: str | None = None


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.post("/linear/issues", response_model=CreateIssueResponse)
async def create_linear_issue(req: CreateIssueRequest):
    if not LINEAR_API_KEY:
        raise HTTPException(status_code=500, detail="LINEAR_API_KEY not configured")

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
            "title": req.title,
            "description": req.description,
            "priority": req.priority,
        }
    }

    if req.due_date:
        variables["input"]["dueDate"] = req.due_date

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
        return CreateIssueResponse(
            success=False,
            error=result["errors"][0].get("message", "Unknown error"),
        )

    issue = result.get("data", {}).get("issueCreate", {}).get("issue", {})
    return CreateIssueResponse(
        success=True,
        identifier=issue.get("identifier"),
        url=issue.get("url"),
    )
