#!/usr/bin/env python3
"""
Propify API sync - fetches data from Propify REST API and stores in Postgres

Propify API notes:
- tickets: capped at 5k, `limit` works, `offset` ignored (~317/day, so 1k = 3 day buffer)
- processes (38k), person-events (71k): full dump only, no pagination/filtering
- all other endpoints: small reference data, full dump

Usage: cd current-be && uv run python scripts/sync_propify.py
"""

import requests
from datetime import datetime
import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/.env"), override=False)

REST_API = 'https://admin.propify.com/rest'
DATABASE_URL = os.environ['DATABASE_URL']
SCHEMA = 'propify'
EMAIL = os.environ['PROPIFY_USERNAME']
PASSWORD = os.environ['PROPIFY_PASSWORD']

def login():
    resp = requests.post(f"{REST_API}/auth/login", json={"username": EMAIL, "password": PASSWORD})
    resp.raise_for_status()
    return resp.json()["accessToken"]

def fetch_json(token, endpoint):
    resp = requests.get(f"{REST_API}/{endpoint}", headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()

ENDPOINTS = [
    ('properties', 'property_snapshot'),
    ('units', 'unit_snapshot'),
    ('utilities', 'utility_snapshot'),
    ('brands', 'brand_snapshot'),
    ('funds', 'fund_snapshot'),
    ('ventures', 'venture_snapshot'),
    ('holding-companies', 'holding_company_snapshot'),
    # ('processes', 'process_snapshot'),
    # ('person-events', 'person_event_snapshot'),
    # ('ticket-groups', 'ticket_group_snapshot'),
    # ('user-groups', 'user_group_snapshot'),
    # ('amenity-types', 'amenity_type_snapshot'),
    # ('pet-policies', 'pet_policy_snapshot'),
    # ('tickets?limit=1000', 'ticket_snapshot'),
]

VIEWS = {
    'property': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            (j.value->>'version')::integer AS version,
            (j.value->>'createLoginId')::integer AS create_login_id,
            j.value->>'createTime' AS create_time,
            (j.value->>'brandId')::integer AS brand_id,
            (j.value->'address'->>'id')::integer AS address__id,
            j.value->'address'->>'addr1' AS address__addr1,
            j.value->'address'->>'city' AS address__city,
            j.value->'address'->>'state' AS address__state,
            j.value->'address'->>'postalCode' AS address__postal_code,
            j.value->'address'->>'country' AS address__country,
            (j.value->'address'->'location'->>'latitude')::float AS address__latitude,
            (j.value->'address'->'location'->>'longitude')::float AS address__longitude,
            j.value->>'transactionStatus' AS transaction_status,
            j.value->>'type' AS type,
            (j.value->>'stories')::integer AS stories,
            (j.value->>'fundId')::integer AS fund_id,
            (j.value->>'ventureId')::integer AS venture_id,
            j.value->>'msa' AS msa,
            j.value->>'county' AS county,
            j.value->>'description' AS description,
            (j.value->>'holdingCompanyId')::integer AS holding_company_id,
            (j.value->>'yearBuilt')::integer AS year_built,
            j.value->>'marketingDescription' AS marketing_description,
            j.value->>'timezone' AS timezone,
            j.value->>'acquisitionDate' AS acquisition_date,
            (j.value->>'safeMode')::boolean AS safe_mode,
            j.value->>'foreignDbId' AS foreign_db_id,
            j.value->>'foreignDbCode' AS foreign_db_code,
            (j.value->>'pool')::boolean AS pool,
            (j.value->>'basement')::boolean AS basement,
            j.value->>'floodZone' AS flood_zone,
            (j.value->>'hoa')::boolean AS hoa,
            j.value->>'listingImages' AS listing_images,
            (j.value->'valuation'->>'amount')::numeric AS valuation__amount,
            j.value->'valuation'->>'date' AS valuation__date,
            j.value->'valuation'->>'source' AS valuation__source,
            (j.value->'valuation'->>'filesCount')::integer AS valuation__files_count,
            j.value->>'unitStatus' AS unit_status,
            (j.value->>'unitId')::integer AS unit_id,
            j.value->>'unitListingStatus' AS unit_listing_status,
            (j.value->>'unitCurrentRent')::numeric AS unit_current_rent,
            (j.value->>'unitListedRent')::numeric AS unit_listed_rent,
            j.value->>'propertyGroupIds' AS property_group_ids,
            (j.value->>'hasLeaseDocumentTemplates')::boolean AS has_lease_document_templates,
            (j.value->>'lastRelevantLeaseId')::integer AS last_relevant_lease_id,
            j.value->>'lastRelevantLeaseStatus' AS last_relevant_lease_status,
            j.value->>'lastRelevantLeaseStartDate' AS last_relevant_lease_start_date,
            j.value->>'lastRelevantLeaseEndDate' AS last_relevant_lease_end_date
        FROM (SELECT json FROM propify.property_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'unit': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            (j.value->>'version')::integer AS version,
            (j.value->>'createLoginId')::integer AS create_login_id,
            j.value->>'createTime' AS create_time,
            (j.value->>'propertyId')::integer AS property_id,
            (j.value->'property'->>'id')::integer AS property__id,
            j.value->'property'->'address'->>'addr1' AS property__address__addr1,
            j.value->'property'->'address'->>'city' AS property__address__city,
            j.value->'property'->'address'->>'state' AS property__address__state,
            j.value->'property'->'address'->>'postalCode' AS property__address__postal_code,
            (j.value->'property'->'address'->'location'->>'latitude')::float AS property__address__latitude,
            (j.value->'property'->'address'->'location'->>'longitude')::float AS property__address__longitude,
            j.value->'property'->>'transactionStatus' AS property__transaction_status,
            j.value->'property'->>'type' AS property__type,
            (j.value->'property'->>'ventureId')::integer AS property__venture_id,
            (j.value->'property'->>'holdingCompanyId')::integer AS property__holding_company_id,
            (j.value->'property'->>'yearBuilt')::integer AS property__year_built,
            j.value->'property'->>'foreignDbCode' AS property__foreign_db_code,
            j.value->>'status' AS status,
            (j.value->>'stories')::integer AS stories,
            j.value->>'statusUpdateTime' AS status_update_time,
            j.value->>'description' AS description,
            (j.value->>'bedroomCount')::integer AS bedroom_count,
            (j.value->>'fullBathroomCount')::integer AS full_bathroom_count,
            (j.value->>'halfBathroomCount')::integer AS half_bathroom_count,
            (j.value->>'quarterBathroomCount')::integer AS quarter_bathroom_count,
            (j.value->>'threeQuarterBathroomCount')::integer AS three_quarter_bathroom_count,
            (j.value->>'squareFeet')::integer AS square_feet,
            (j.value->>'rentPrice')::numeric AS rent_price,
            (j.value->>'ready')::boolean AS ready,
            (j.value->>'rented')::boolean AS rented,
            (j.value->>'notice')::boolean AS notice,
            j.value->>'readyDate' AS ready_date,
            j.value->>'amenities' AS amenities,
            j.value->>'foreignApplicationLink' AS foreign_application_link,
            (j.value->>'marketRent')::numeric AS market_rent,
            (j.value->>'proFormaRent')::numeric AS pro_forma_rent,
            j.value->>'listingStatus' AS listing_status,
            (j.value->>'smartHomeSetupEnabled')::boolean AS smart_home_setup_enabled,
            j.value->>'foreignDbId' AS foreign_db_id,
            j.value->>'foreignDbCode' AS foreign_db_code,
            (j.value->>'listedRent')::numeric AS listed_rent,
            (j.value->>'pricingCadenceDays')::integer AS pricing_cadence_days,
            (j.value->>'companyId')::integer AS company_id,
            (j.value->>'officeId')::integer AS office_id,
            (j.value->>'singleApplication')::boolean AS single_application,
            (j.value->>'hasLeaseDocumentTemplates')::boolean AS has_lease_document_templates,
            (j.value->>'validKeyedMailbox')::boolean AS valid_keyed_mailbox,
            (j.value->>'validOfficeId')::boolean AS valid_office_id
        FROM (SELECT json FROM propify.unit_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'utility': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            (j.value->>'version')::integer AS version,
            (j.value->>'createLoginId')::integer AS create_login_id,
            j.value->>'createTime' AS create_time,
            (j.value->>'propertyId')::integer AS property_id,
            (j.value->>'unitId')::integer AS unit_id,
            (j.value->>'ventureId')::integer AS venture_id,
            j.value->>'type' AS type,
            j.value->>'status' AS status,
            j.value->>'responsiblePartyRoleType' AS responsible_party_role_type,
            j.value->>'vendorName' AS vendor_name,
            j.value->>'vendorContact' AS vendor_contact,
            j.value->>'accountName' AS account_name,
            j.value->>'accountNumber' AS account_number,
            j.value->>'meterNumber' AS meter_number,
            j.value->>'accountStartDate' AS account_start_date,
            j.value->>'accountStopDate' AS account_stop_date,
            (j.value->>'active')::boolean AS active,
            (j.value->>'onBillingProgram')::boolean AS on_billing_program,
            NULLIF(NULLIF(j.value->>'adminFee', 'false'), 'true')::numeric AS admin_fee,
            (j.value->>'sentToUtilityManagement')::boolean AS sent_to_utility_management,
            j.value->>'foreignDbCode' AS foreign_db_code,
            j.value->'propertyAddress'->>'addr1' AS property_address__addr1,
            j.value->'propertyAddress'->>'city' AS property_address__city,
            j.value->'propertyAddress'->>'state' AS property_address__state,
            j.value->'propertyAddress'->>'postalCode' AS property_address__postal_code,
            (j.value->'propertyAddress'->'location'->>'latitude')::float AS property_address__latitude,
            (j.value->'propertyAddress'->'location'->>'longitude')::float AS property_address__longitude,
            (j.value->'lastNote'->>'id')::integer AS last_note__id,
            j.value->'lastNote'->>'note' AS last_note__note,
            j.value->'lastNote'->>'createTime' AS last_note__create_time
        FROM (SELECT json FROM propify.utility_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'venture': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            j.value->>'name' AS name
        FROM (SELECT json FROM propify.venture_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'fund': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            j.value->>'name' AS name,
            (j.value->>'ventureId')::integer AS venture_id
        FROM (SELECT json FROM propify.fund_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'process': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            (j.value->>'version')::integer AS version,
            (j.value->>'createLoginId')::integer AS create_login_id,
            j.value->>'createTime' AS create_time,
            j.value->>'finalizedTime' AS finalized_time,
            j.value->>'status' AS status,
            j.value->>'processType' AS process_type,
            (j.value->>'processId')::integer AS process_id
        FROM (SELECT json FROM propify.process_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
    'person_event': '''
        SELECT
            (j.value->>'id')::integer AS id,
            (j.value->>'orgId')::integer AS org_id,
            (j.value->>'createLoginId')::integer AS create_login_id,
            j.value->>'createTime' AS create_time,
            (j.value->>'personId')::integer AS person_id,
            j.value->>'eventName' AS event_name,
            j.value->>'eventDate' AS event_date,
            j.value->>'occurredDate' AS occurred_date,
            j.value->>'appliedDate' AS applied_date,
            j.value->>'signedDate' AS signed_date,
            j.value->>'moveInDate' AS move_in_date,
            j.value->>'moveOutDate' AS move_out_date,
            j.value->>'leaseFromDate' AS lease_from_date,
            j.value->>'leaseToDate' AS lease_to_date,
            (j.value->>'rentPrice')::numeric AS rent_price,
            (j.value->>'rentDeposit')::numeric AS rent_deposit,
            (j.value->>'unitId')::integer AS unit_id,
            (j.value->>'monthToMonth')::boolean AS month_to_month,
            j.value->>'foreignDbId' AS foreign_db_id,
            (j.value->'unit'->>'id')::integer AS unit__id,
            (j.value->'unit'->>'propertyId')::integer AS unit__property_id,
            (j.value->'unit'->'property'->>'id')::integer AS unit__property__id,
            j.value->'unit'->'property'->'address'->>'addr1' AS unit__property__address__addr1,
            j.value->'unit'->'property'->'address'->>'city' AS unit__property__address__city,
            j.value->'unit'->'property'->'address'->>'state' AS unit__property__address__state,
            j.value->'unit'->>'description' AS unit__description,
            (j.value->'resident'->>'id')::integer AS resident__id,
            j.value->'resident'->>'firstName' AS resident__first_name,
            j.value->'resident'->>'lastName' AS resident__last_name,
            j.value->'resident'->>'type' AS resident__type,
            j.value->'resident'->>'status' AS resident__status,
            (j.value->'resident'->>'balance')::numeric AS resident__balance,
            j.value->'resident'->>'foreignDbCode' AS resident__foreign_db_code,
            (j.value->'person'->>'id')::integer AS person__id,
            j.value->'person'->>'firstName' AS person__first_name,
            j.value->'person'->>'lastName' AS person__last_name,
            j.value->'person'->>'partyType' AS person__party_type
        FROM (SELECT json FROM propify.person_event_snapshot ORDER BY ts DESC LIMIT 1) s, jsonb_array_elements(s.json) j(value)
    ''',
}

def create_materialized_views(cur):
    for name, sql in VIEWS.items():
        cur.execute(f'DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.{name}')
        cur.execute(f'CREATE MATERIALIZED VIEW {SCHEMA}.{name} AS {sql}')

def refresh_materialized_views(cur):
    for name in VIEWS:
        cur.execute(f'REFRESH MATERIALIZED VIEW {SCHEMA}.{name}')

def sync():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')

    for _, table in ENDPOINTS:
        cur.execute(f'CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} (ts TIMESTAMPTZ, json JSONB)')
    conn.commit()

    print("Logging in...")
    token = login()
    print("Logged in")

    ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    for endpoint, table in ENDPOINTS:
        print(f"Fetching {endpoint}...")
        data = fetch_json(token, endpoint)
        cur.execute(f'INSERT INTO {SCHEMA}.{table} (ts, json) VALUES (%s, %s)', (ts, json.dumps(data)))
        print(f"  {len(data)} records")

    conn.commit()

    # Check if materialized views exist, create or refresh accordingly
    cur.execute("""
        SELECT COUNT(*) FROM pg_matviews WHERE schemaname = %s
    """, (SCHEMA,))
    matview_count = cur.fetchone()[0]

    if matview_count < len(VIEWS):
        print("Creating materialized views...")
        create_materialized_views(cur)
    else:
        print("Refreshing materialized views...")
        refresh_materialized_views(cur)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Done at {ts}")

if __name__ == "__main__":
    sync()
