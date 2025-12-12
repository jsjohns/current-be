#!/usr/bin/env python3
"""
Linear GraphQL sync - fetches all Linear data via GraphQL and stores in Postgres

Syncs teams, users, projects, issues, workflow_states, issue_labels, cycles,
attachments, comments, and issue_relations.

Usage: cd current-be && uv run python scripts/sync_linear.py
"""

import os
import json
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

DATABASE_URL = os.environ['DATABASE_URL']
LINEAR_API_KEY = os.environ['LINEAR_API_KEY']
SCHEMA = 'linear'
API_URL = 'https://api.linear.app/graphql'

def gql(query, variables=None):
    """Execute a GraphQL query against Linear API"""
    resp = requests.post(
        API_URL,
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

def fetch_all(query_name, query, node_fields):
    """Fetch all pages of a paginated query"""
    all_nodes = []
    cursor = None

    while True:
        q = f"""
        query($cursor: String) {{
            {query_name}(first: 100, after: $cursor) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    {node_fields}
                }}
            }}
        }}
        """
        data = gql(q, {'cursor': cursor})
        result = data[query_name]
        all_nodes.extend(result['nodes'])

        if not result['pageInfo']['hasNextPage']:
            break
        cursor = result['pageInfo']['endCursor']

    return all_nodes

# Define what to sync - query name, table name, fields
SYNCS = {
    'teams': {
        'query': 'teams',
        'fields': '''
            id
            key
            name
            color
            icon
            private
            timezone
            createdAt
            updatedAt
            issueCount
            scimManaged
            cyclesEnabled
            triageEnabled
            cycleDuration
            cycleStartDay
            cycleCooldownTime
            cycleLockToActive
            upcomingCycleCount
            autoClosePeriod
            autoCloseStateId
            autoArchivePeriod
            groupIssueHistory
            issueEstimationType
            defaultIssueEstimate
            issueEstimationExtended
            issueEstimationAllowZero
            setIssueSortOrderOnStateChange
            cycleIssueAutoAssignStarted
            cycleIssueAutoAssignCompleted
            requirePriorityToLeaveTriage
            cycleCalenderUrl
            parent { id }
            activeCycle { id }
            defaultIssueState { id }
            triageIssueState { id }
            markedAsDuplicateWorkflowState { id }
        '''
    },
    'users': {
        'query': 'users',
        'fields': '''
            id
            name
            email
            displayName
            avatarUrl
            avatarBackgroundColor
            initials
            active
            admin
            guest
            isMe
            timezone
            lastSeen
            createdAt
            updatedAt
            createdIssueCount
            inviteHash
            url
        '''
    },
    'projects': {
        'query': 'projects',
        'fields': '''
            id
            name
            description
            icon
            color
            state
            progress
            health
            targetDate
            startDate
            startedAt
            createdAt
            updatedAt
            canceledAt
            completedAt
            healthUpdatedAt
            slugId
            url
            scope
            sortOrder
            priority
            prioritySortOrder
            content
            contentState
            scopeHistory
            issueCountHistory
            completedScopeHistory
            inProgressScopeHistory
            completedIssueCountHistory
            updateRemindersDay
            updateRemindersHour
            lead { id }
            creator { id }
            status { id }
            convertedFromIssue { id }
        '''
    },
    'issues': {
        'query': 'issues',
        'fields': '''
            id
            identifier
            title
            description
            descriptionState
            priority
            priorityLabel
            prioritySortOrder
            estimate
            dueDate
            createdAt
            updatedAt
            canceledAt
            completedAt
            startedAt
            addedToTeamAt
            addedToCycleAt
            addedToProjectAt
            url
            branchName
            number
            sortOrder
            subIssueSortOrder
            slaType
            customerTicketCount
            integrationSourceType
            previousIdentifiers
            reactionData
            labelIds
            team { id }
            project { id }
            projectMilestone { id }
            assignee { id }
            creator { id }
            parent { id }
            cycle { id }
            state { id }
            sourceComment { id }
            labels { nodes { id } }
            subscribers { nodes { id } }
            relations { nodes { id } }
            attachments { nodes { id } }
        '''
    },
    'workflow_states': {
        'query': 'workflowStates',
        'fields': '''
            id
            name
            color
            type
            position
            description
            createdAt
            updatedAt
            team { id }
            inheritedFrom { id }
        '''
    },
    'issue_labels': {
        'query': 'issueLabels',
        'fields': '''
            id
            name
            color
            description
            isGroup
            createdAt
            updatedAt
            team { id }
            parent { id }
            creator { id }
            inheritedFrom { id }
        '''
    },
    'cycles': {
        'query': 'cycles',
        'fields': '''
            id
            number
            name
            description
            startsAt
            endsAt
            completedAt
            progress
            createdAt
            updatedAt
            team { id }
        '''
    },
    'attachments': {
        'query': 'attachments',
        'fields': '''
            id
            title
            subtitle
            url
            sourceType
            groupBySource
            createdAt
            updatedAt
            issue { id }
            creator { id }
        '''
    },
    'comments': {
        'query': 'comments',
        'fields': '''
            id
            body
            bodyData
            url
            createdAt
            updatedAt
            editedAt
            issue { id }
            user { id }
            parent { id }
            resolvingUser { id }
            resolvingComment { id }
        '''
    },
    'issue_relations': {
        'query': 'issueRelations',
        'fields': '''
            id
            type
            createdAt
            updatedAt
            issue { id }
            relatedIssue { id }
        '''
    },
}

def flatten_node(node):
    """Flatten nested objects like { id } to just the id value"""
    flat = {}
    for key, value in node.items():
        if isinstance(value, dict):
            if 'id' in value and len(value) <= 3:  # Simple reference object
                flat[f"{key}_id"] = value['id']
                # Include extra fields if present (like state.name, state.type)
                for k, v in value.items():
                    if k != 'id':
                        flat[f"{key}_{k}"] = v
            elif 'nodes' in value:  # Array relation
                flat[key] = json.dumps([n.get('id') or n for n in value['nodes']])
            else:
                flat[key] = json.dumps(value)
        elif isinstance(value, list):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat

def sync_table(cur, table_name, nodes):
    """Sync nodes to a table"""
    if not nodes:
        print(f"  No data for {table_name}")
        return

    # Flatten all nodes
    flat_nodes = [flatten_node(n) for n in nodes]

    # Get all columns from the data
    columns = set()
    for node in flat_nodes:
        columns.update(node.keys())
    columns = sorted(columns)

    # Drop and recreate table
    cur.execute(f'DROP TABLE IF EXISTS {SCHEMA}.{table_name}')

    # Create table with TEXT columns (simple approach)
    col_defs = ', '.join(f'"{c}" TEXT' for c in columns)
    cur.execute(f'CREATE TABLE {SCHEMA}.{table_name} ({col_defs})')

    # Insert data
    rows = []
    for node in flat_nodes:
        row = tuple(str(node.get(c)) if node.get(c) is not None else None for c in columns)
        rows.append(row)

    col_names = ', '.join(f'"{c}"' for c in columns)
    placeholders = ', '.join(['%s'] * len(columns))
    cur.executemany(
        f'INSERT INTO {SCHEMA}.{table_name} ({col_names}) VALUES ({placeholders})',
        rows
    )

    print(f"  {table_name}: {len(rows)} rows, {len(columns)} columns")

def sync():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Create schema
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
    conn.commit()

    print("Syncing Linear data...")

    for table_name, config in SYNCS.items():
        print(f"Fetching {table_name}...")
        try:
            nodes = fetch_all(config['query'], None, config['fields'])
            sync_table(cur, table_name, nodes)
            conn.commit()
        except Exception as e:
            print(f"  Error: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    sync()
