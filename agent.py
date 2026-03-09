"""
Deal Association Agent - Domain-First Matching
Strategy:
  1. Extract key terms from deal name
  2. Search companies by domain (primary) and name (fallback)
  3. Find all contacts whose email domain matches the company domain
  4. Associate deal → company + contacts
  5. Webhook endpoint for new deals, bulk backfill endpoint for existing
"""

import os
import re
import json
import requests
from datetime import datetime, timezone

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
BASE    = "https://api.hubapi.com"
HEADERS = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

# Common words to strip when building search terms from deal names
NOISE_WORDS = {
    "insurance", "agency", "group", "services", "llc", "inc", "corp",
    "company", "co", "the", "and", "of", "associates", "advisors",
    "brokers", "partners", "solutions", "consulting", "financial",
    "independent", "national", "american", "united", "professional",
}

def hs_get(path, params=None):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def hs_post(path, payload):
    r = requests.post(f"{BASE}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

def hs_put(path, payload):
    r = requests.put(f"{BASE}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

# ── Matching helpers ───────────────────────────────────────────────────────

def extract_keywords(deal_name):
    """Extract meaningful keywords from a deal name for domain search."""
    words = re.sub(r"[^a-zA-Z0-9\s]", "", deal_name.lower()).split()
    keywords = [w for w in words if w not in NOISE_WORDS and len(w) > 2]
    return keywords

def find_company_by_domain_keyword(deal_name):
    """
    Search companies whose domain contains keywords from the deal name.
    Returns the best matching company or None.
    """
    keywords = extract_keywords(deal_name)
    if not keywords:
        return None

    # Try combinations of keywords as domain search (most specific first)
    search_terms = []
    if len(keywords) >= 2:
        search_terms.append("".join(keywords[:2]))   # e.g. "philipsinsurance"
        search_terms.append(keywords[0] + keywords[-1])
    search_terms.extend(keywords)                     # individual keywords

    for term in search_terms:
        try:
            data = hs_post("/crm/v3/objects/companies/search", {
                "filterGroups": [{"filters": [
                    {"propertyName": "domain", "operator": "CONTAINS_TOKEN", "value": term}
                ]}],
                "properties": ["name", "domain"],
                "limit": 5,
            })
            results = data.get("results", [])
            for r in results:
                domain = (r["properties"].get("domain") or "").lower()
                name   = (r["properties"].get("name") or "").lower()
                deal_lower = deal_name.lower()
                # Validate: keyword appears in domain AND name is plausibly related
                if term in domain and (
                    any(k in name for k in keywords) or
                    any(k in domain for k in keywords)
                ):
                    return r
        except:
            continue

    return None

def find_company_by_name(deal_name):
    """Fallback: search companies by name."""
    # Try exact match first
    data = hs_post("/crm/v3/objects/companies/search", {
        "filterGroups": [{"filters": [
            {"propertyName": "name", "operator": "EQ", "value": deal_name}
        ]}],
        "properties": ["name", "domain"],
        "limit": 1,
    })
    results = data.get("results", [])
    if results:
        return results[0]

    # Fuzzy text search
    data = hs_post("/crm/v3/objects/companies/search", {
        "query": deal_name,
        "properties": ["name", "domain"],
        "limit": 5,
    })
    keywords = extract_keywords(deal_name)
    for r in data.get("results", []):
        cname = (r["properties"].get("name") or "").lower()
        if any(k in cname for k in keywords):
            return r
    return None

def find_contacts_by_email_domain(domain):
    """Find all contacts whose email domain matches the company domain."""
    if not domain:
        return []
    # Strip protocol/www from domain
    domain = re.sub(r"^https?://", "", domain).strip("/").lstrip("www.")

    try:
        data = hs_post("/crm/v3/objects/contacts/search", {
            "filterGroups": [{"filters": [
                {"propertyName": "hs_email_domain", "operator": "EQ", "value": domain}
            ]}],
            "properties": ["firstname", "lastname", "email", "hs_email_domain"],
            "limit": 10,
        })
        return data.get("results", [])
    except:
        return []

def get_deal_associations(deal_id):
    """Get existing company and contact associations for a deal."""
    companies, contacts = [], []
    try:
        r = hs_get(f"/crm/v4/objects/deals/{deal_id}/associations/companies")
        companies = [x["toObjectId"] for x in r.get("results", [])]
    except: pass
    try:
        r = hs_get(f"/crm/v4/objects/deals/{deal_id}/associations/contacts")
        contacts = [x["toObjectId"] for x in r.get("results", [])]
    except: pass
    return companies, contacts

def associate_deal_to_company(deal_id, company_id):
    hs_put(f"/crm/v4/objects/deals/{deal_id}/associations/companies/{company_id}", [
        {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 341}
    ])

def associate_deal_to_contact(deal_id, contact_id):
    hs_put(f"/crm/v4/objects/deals/{deal_id}/associations/contacts/{contact_id}", [
        {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}
    ])

# ── Core association logic ─────────────────────────────────────────────────

def associate_deal(deal_id, deal_name, verbose=True):
    """
    Associate a single deal with its company and contacts.
    Uses domain-first matching, falls back to name matching.
    """
    result = {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "match_method": None,
        "company_id": None,
        "company_name": None,
        "company_domain": None,
        "contacts_associated": 0,
        "skipped": None,
    }

    if not deal_name or not deal_name.strip():
        result["skipped"] = "No deal name"
        return result

    # Check existing associations
    existing_companies, existing_contacts = get_deal_associations(deal_id)
    already_complete = len(existing_companies) > 0 and len(existing_contacts) > 0
    if already_complete:
        result["skipped"] = "Already has company and contacts"
        return result

    company = None

    # ── Step 1: Domain-first company search ───────────────────────────────
    if not existing_companies:
        company = find_company_by_domain_keyword(deal_name)
        if company:
            result["match_method"] = "domain"
        else:
            # Fallback to name search
            company = find_company_by_name(deal_name)
            if company:
                result["match_method"] = "name"

        if company:
            try:
                associate_deal_to_company(deal_id, company["id"])
                result["company_id"]     = company["id"]
                result["company_name"]   = company["properties"].get("name")
                result["company_domain"] = company["properties"].get("domain")
                if verbose:
                    print(f"  🏢 [{result['match_method'].upper()}] Company: {result['company_name']} ({result['company_domain']})")
            except Exception as e:
                if verbose:
                    print(f"  ⚠️ Company assoc failed: {e}")
        else:
            if verbose:
                print(f"  ❌ No company found for: {deal_name}")
    else:
        # Already has company — get its domain to find contacts
        try:
            co_data = hs_get(f"/crm/v3/objects/companies/{existing_companies[0]}?properties=name,domain")
            company = {"id": existing_companies[0], "properties": co_data.get("properties", {})}
            result["company_id"]     = existing_companies[0]
            result["company_name"]   = company["properties"].get("name")
            result["company_domain"] = company["properties"].get("domain")
            result["match_method"]   = "existing"
        except: pass

    # ── Step 2: Find contacts by email domain ──────────────────────────────
    if not existing_contacts and company:
        domain = (company["properties"].get("domain") or "").strip()
        if domain:
            contacts = find_contacts_by_email_domain(domain)
            for contact in contacts:
                try:
                    associate_deal_to_contact(deal_id, contact["id"])
                    result["contacts_associated"] += 1
                    name = f"{contact['properties'].get('firstname','')} {contact['properties'].get('lastname','')}".strip()
                    email = contact['properties'].get('email','')
                    if verbose:
                        print(f"  👤 Contact: {name} ({email})")
                except Exception as e:
                    if verbose:
                        print(f"  ⚠️ Contact assoc failed: {e}")
        else:
            if verbose:
                print(f"  ⚠️ Company has no domain — can't find contacts")

    return result

# ── Bulk backfill ──────────────────────────────────────────────────────────

def backfill_all():
    """Find all deals missing company OR contact associations and fix them."""
    print("🔍 Fetching deals missing associations...")
    deals, after = [], None
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "num_associated_contacts", "operator": "EQ", "value": "0"},
            ]}],
            "properties": ["dealname"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        data = hs_post("/crm/v3/objects/deals/search", body)
        deals.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or len(deals) >= 500:
            break

    print(f"📊 Found {len(deals)} deals missing associations\n")

    stats = {
        "total": len(deals),
        "companies_associated": 0,
        "contacts_associated": 0,
        "no_match_found": 0,
        "skipped": 0,
        "match_by_domain": 0,
        "match_by_name": 0,
    }

    for deal in deals:
        deal_id   = deal["id"]
        deal_name = deal["properties"].get("dealname", "") or ""
        print(f"🔗 {deal_name or 'Unnamed'} ({deal_id})")

        r = associate_deal(deal_id, deal_name)

        if r.get("skipped"):
            stats["skipped"] += 1
        elif r.get("company_id"):
            stats["companies_associated"] += 1
            stats["contacts_associated"]  += r["contacts_associated"]
            if r["match_method"] == "domain":
                stats["match_by_domain"] += 1
            elif r["match_method"] == "name":
                stats["match_by_name"] += 1
        else:
            stats["no_match_found"] += 1

    print(f"\n🎉 Backfill complete!")
    print(json.dumps(stats, indent=2))
    return stats

if __name__ == "__main__":
    backfill_all()
