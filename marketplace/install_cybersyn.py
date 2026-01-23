#!/usr/bin/env python3
"""
Marketplace Listing Installation Script

This script programmatically installs free Snowflake Marketplace listings
using the Snowflake REST API.

Usage:
    python3 install_cybersyn.py --pat-token <token> --account <account>

Prerequisites:
    - Snowflake PAT token with ACCOUNTADMIN or appropriate privileges
    - pip install requests
"""

import argparse
import requests
import json
import sys

# Known free marketplace listing Global Names (ULLs)
RECOMMENDED_LISTINGS = {
    "cybersyn_economic": {
        "name": "Cybersyn Economic Data",
        "description": "Free FRED economic indicators, commodities, industry data",
        "global_name": "GZTYZK2WB0",  # This is the listing global name - may need update
        "use_case": "Crude oil prices, natural gas, PPI indexes for cost tracking"
    },
    # Add more listings as discovered
}

def get_snowflake_headers(pat_token: str) -> dict:
    """Create headers for Snowflake REST API calls."""
    return {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def get_listing_info(host: str, pat_token: str, listing_global_name: str) -> dict:
    """Get information about a marketplace listing."""
    url = f"https://{host}/api/v2/marketplace/listings/{listing_global_name}"
    headers = get_snowflake_headers(pat_token)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error getting listing info: {e}")
        print(f"Response: {response.text if response else 'No response'}")
        return None

def request_listing(host: str, pat_token: str, listing_global_name: str, database_name: str) -> dict:
    """Request/install a marketplace listing."""
    url = f"https://{host}/api/v2/marketplace/listings/{listing_global_name}/request"
    headers = get_snowflake_headers(pat_token)
    
    payload = {
        "database_name": database_name
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error requesting listing: {e}")
        print(f"Response: {response.text if response else 'No response'}")
        return None

def list_installed_listings(host: str, pat_token: str) -> list:
    """List all installed marketplace listings."""
    # This uses SQL via the REST API
    url = f"https://{host}/api/v2/statements"
    headers = get_snowflake_headers(pat_token)
    
    payload = {
        "statement": "SHOW SHARES;",
        "timeout": 60,
        "database": "SNOWFLAKE",
        "warehouse": "COMPUTE_WH"
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error listing shares: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Install Snowflake Marketplace listings")
    parser.add_argument("--pat-token", required=True, help="Snowflake PAT token")
    parser.add_argument("--host", default="sfpscogs-rraman-aws-si.snowflakecomputing.com", 
                       help="Snowflake account host")
    parser.add_argument("--list", action="store_true", help="List recommended listings")
    parser.add_argument("--install", type=str, help="Install a listing by key name")
    parser.add_argument("--database-name", type=str, help="Name for the installed database")
    
    args = parser.parse_args()
    
    if args.list:
        print("\n=== Recommended Free Marketplace Listings ===\n")
        for key, info in RECOMMENDED_LISTINGS.items():
            print(f"Key: {key}")
            print(f"  Name: {info['name']}")
            print(f"  Description: {info['description']}")
            print(f"  Use Case: {info['use_case']}")
            print(f"  Global Name: {info['global_name']}")
            print()
        return
    
    if args.install:
        if args.install not in RECOMMENDED_LISTINGS:
            print(f"Unknown listing key: {args.install}")
            print(f"Available: {list(RECOMMENDED_LISTINGS.keys())}")
            return
        
        listing = RECOMMENDED_LISTINGS[args.install]
        database_name = args.database_name or listing['name'].upper().replace(" ", "_")
        
        print(f"Installing: {listing['name']}")
        print(f"Database will be: {database_name}")
        
        # Get listing info first
        info = get_listing_info(args.host, args.pat_token, listing['global_name'])
        if info:
            print(f"Listing found: {json.dumps(info, indent=2)}")
        
        # Request the listing
        result = request_listing(args.host, args.pat_token, listing['global_name'], database_name)
        if result:
            print(f"Success: {json.dumps(result, indent=2)}")
        else:
            print("Failed to install listing")
            print("\n=== Manual Installation Instructions ===")
            print("1. Go to Snowsight: https://app.snowflake.com")
            print("2. Navigate to: Data > Marketplace")
            print("3. Search for: Cybersyn")
            print("4. Click 'Get' on: 'Financial & Economic Essentials'")
            print("5. Accept terms and create database")
            print("6. Run: marketplace/create_marketplace_views.sql")
        return
    
    # Default: show help
    parser.print_help()

if __name__ == "__main__":
    main()
