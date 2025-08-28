#!/usr/bin/env python3
"""
Extract only party patterns from the detailed mappings file.
"""

import json

# Load the detailed mappings
with open('extracted_mappings/detailed_mappings.json', 'r') as f:
    data = json.load(f)

# Extract only party patterns
party_only = {
    "party_patterns": data["party_patterns"],
    "summary": {
        "total_party_patterns": len(data["party_patterns"]),
        "states_with_party_mappings": 30,
        "extraction_date": "2024-12-19",
        "notes": "Only party mappings included. Office and county mappings remain state-specific."
    }
}

# Save to new file
with open('extracted_mappings/party_mappings_only.json', 'w') as f:
    json.dump(party_only, f, indent=2)

print("Party mappings extracted successfully!")
print(f"Total party patterns: {len(data['party_patterns'])}")
