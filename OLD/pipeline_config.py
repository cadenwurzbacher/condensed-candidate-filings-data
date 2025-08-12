#!/usr/bin/env python3
"""
Configuration file for the Enterprise Political Candidates Data Pipeline.

This file contains all configuration settings, mappings, and constants used
throughout the pipeline to ensure consistency and maintainability.
"""

import os
from datetime import datetime

# Pipeline Configuration
PIPELINE_VERSION = "1.0.0"
PIPELINE_NAME = "Enterprise Political Candidates Data Pipeline"

# File Paths
INPUT_DIRECTORY = 'state_data'
LOG_DIRECTORY = 'logs'

# Ensure directories exist
for directory in [LOG_DIRECTORY]:
    os.makedirs(directory, exist_ok=True)

# Supabase Configuration
SUPABASE_CONFIG = {
    'url': "https://bnvpsoppbufldabquoec.supabase.co",
    'key': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJudnBzb3BwYnVmbGRhYnF1b2VjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTc2NzYyMCwiZXhwIjoyMDY3MzQzNjIwfQ.4phfSXHEpuA35nGbpxNBuXmIpcypmviFisakLIWRDAE",
    'connection_string': "postgresql://postgres.bnvpsoppbufldabquoec:Undsioux1!!!@aws-0-us-east-2.pooler.supabase.com:5432/postgres",
    'tables': {
        'staging': 'staging_candidates',
        'production': 'filings'
    }
}

# Standard Column Definitions - Updated to match actual Supabase schema
STANDARD_COLUMNS = {
    # Core fields (matching Supabase)
    'id': 'ID',
    'stable_id': 'Stable ID',
    'state': 'State',
    'election_year': 'Election Year',
    'office': 'Office',
    'party': 'Party',
    'district': 'District',
    'county': 'County',
    'city': 'City',
    'address': 'Address',
    'zip_code': 'Zip Code',
    'phone': 'Phone',
    'email': 'Email',
    'website': 'Website',
    'filing_date': 'Filing Date',
    'election_date': 'Election Date',
    'election_type': 'Election Type',
    'facebook': 'Facebook',
    'twitter': 'Twitter',
    'created_at': 'Created At',
    'updated_at': 'Updated At',
    'data_version': 'Data Version',
    'batch_id': 'Batch ID',
    
    # Original data preservation fields
    'original_name': 'Original Name',
    'original_state': 'Original State',
    'original_election_year': 'Original Election Year',
    'original_office': 'Original Office',
    'original_filing_date': 'Original Filing Date',
    
    # Parsed name fields
    'first_name': 'First Name',
    'middle_name': 'Middle Name',
    'last_name': 'Last Name',
    'full_name_display': 'Full Name Display',
    
    # Legacy fields (for backward compatibility)
    'candidate_name': 'Candidate Name',
    'source_file': 'Source File'
}

# State Name Mappings
STATE_MAPPING = {
    'AL': 'Alabama',
    'AK': 'Alaska', 
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'DC': 'District of Columbia',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',
    # US Territories
    'AS': 'American Samoa',
    'GU': 'Guam',
    'MP': 'Northern Mariana Islands',
    'PR': 'Puerto Rico',
    'VI': 'U.S. Virgin Islands',
    # Handle common variations
    'New': 'New York',
    'South': 'South Carolina',
    'North': 'North Carolina',
    'West': 'West Virginia'
}

# Party Name Standardization
PARTY_MAPPING = {
    'dem': 'Democratic',
    'democrat': 'Democratic',
    'democratic': 'Democratic',
    'democratic party': 'Democratic',
    'rep': 'Republican',
    'republican': 'Republican',
    'republican party': 'Republican',
    'ind': 'Independent',
    'independent': 'Independent',
    'lib': 'Libertarian',
    'libertarian': 'Libertarian',
    'green': 'Green',
    'green party': 'Green',
    'nonpartisan': 'Nonpartisan',
    'non-partisan': 'Nonpartisan',
    'constitution': 'Constitution',
    'constitution party': 'Constitution',
    'reform': 'Reform',
    'reform party': 'Reform',
    'natural law': 'Natural Law',
    'natural law party': 'Natural Law',
    'socialist': 'Socialist',
    'socialist party': 'Socialist',
    'communist': 'Communist',
    'communist party': 'Communist',
    'progressive': 'Progressive',
    'progressive party': 'Progressive',
    'populist': 'Populist',
    'populist party': 'Populist',
    'tea party': 'Tea Party',
    'tea': 'Tea Party',
    'libertarian party': 'Libertarian',
    'green party of the united states': 'Green',
    'constitution party of the united states': 'Constitution',
    'reform party of the united states': 'Reform'
}

# Column Mapping for Different States
COLUMN_MAPPINGS = {
    # Candidate Name variations
    'candidate_name': [
        'Candidate Name', 'Name', 'Candidate', 'Full Name', 'Name On Ballot',
        'Ballot Name(s)', 'Candidate Name', 'Name', 'name', 'candidate_name',
        'first_name', 'last_name'  # For states with separate name columns
    ],
    
    # State variations
    'state': [
        'State', 'State Name'
    ],
    
    # Year variations
    'election_year': [
        'Year', 'Election Year', 'Election Date', 'election_date'
    ],
    
    # Office variations
    'office': [
        'Office', 'Position', 'Office Sought', 'Race', 'OfficeTitle',
        'OfficeTitleDescription', 'Office Name', 'Office Title', 'Position/Office',
        'office_sought', 'office'
    ],
    
    # Party variations
    'party': [
        'Party', 'Political Party', 'Affiliation', 'Party Preference',
        'Party Affiliation', 'Candidate Party', 'party', 'Party Preference'
    ],
    
    # District variations
    'district': [
        'District', 'District Number', 'District Name', 'District/Circuit',
        'District/County', 'district', 'District Type', 'Dist.'
    ],
    
    # County variations
    'county': [
        'County', 'County Name', 'Filing County', 'District/County'
    ],
    
    # City variations
    'city': [
        'City', 'City Name', 'Home City', 'Town Of Residence'
    ],
    
    # Address variations
    'address': [
        'Address', 'Mailing Address', 'Home Address', 'Campaign Address Line 1',
        'MailingAddress', 'Street Number', 'Street Name', 'Unit/Apt/Suite',
        'Address Line 2', 'Line 2'
    ],
    
    # Zip Code variations
    'zip_code': [
        'Zip', 'Zip Code', 'Home Zip'
    ],
    
    # Phone variations
    'phone': [
        'Phone', 'Phone Number', 'Home Phone', 'Cell Phone', 'Campaign Phone',
        'CampaignPhoneNumber'
    ],
    
    # Email variations
    'email': [
        'Email', 'Email Address', 'Campaign Email'
    ],
    
    # Website variations
    'website': [
        'Website', 'Web Address', 'Campaign Website'
    ],
    
    # Filing Date variations
    'filing_date': [
        'Filing Date', 'Date Filed', 'Registration Date', 'Filed Date',
        'Filing Date/Time'
    ],
    
    # Election Date variations
    'election_date': [
        'Election Date', 'election_date'
    ],
    
    # Election Type variations
    'election_type': [
        'Election Type', 'election_type', 'Election', 'Election Name'
    ],
    
    # Social Media variations
    'facebook': [
        'Facebook'
    ],
    
    'twitter': [
        'Twitter'
    ]
}

# Stable ID Configuration - Updated to match Supabase schema
STABLE_ID_CONFIG = {
    'fields': [
        'original_name', 'original_state', 'original_office', 'party', 'district', 'county', 'city', 
        'address', 'zip_code', 'phone', 'email', 'website', 'original_filing_date', 
        'election_date', 'election_type', 'batch_id'
    ],
    'hash_length': 32,
    'hash_algorithm': 'sha256',
    'include_nulls': True,  # Include null/blank values in ID generation
    'null_placeholder': 'NULL_VALUE'  # Placeholder for null values
}

# Data Validation Rules
VALIDATION_RULES = {
    'phone': {
        'pattern': r'^\d{10}$',
        'description': '10-digit US phone number'
    },
    'email': {
        'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'description': 'Valid email format'
    },
    'zip_code': {
        'pattern': r'^\d{5}(-\d{4})?$',
        'description': '5 or 9-digit US zip code'
    },
    'election_date': {
        'min_date': '1900-01-01',
        'max_date': '2030-12-31',
        'description': 'Date between 1900 and 2030'
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': f'logs/political_pipeline_{datetime.now().strftime("%Y%m%d")}.log',
    'max_file_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5
}

# Performance Configuration
PERFORMANCE_CONFIG = {
    'batch_size': 1000,
    'max_workers': 4,
    'timeout_seconds': 300,
    'retry_attempts': 3,
    'retry_delay_seconds': 5
}

# Error Handling Configuration
ERROR_CONFIG = {
    'continue_on_error': True,
    'log_errors': True,
    'save_error_records': True,
    'error_file': f'logs/errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
}

# Data Quality Thresholds
QUALITY_THRESHOLDS = {
    'minimum_required_fields': ['candidate_name', 'state', 'office'],
    'duplicate_threshold': 0.05,  # 5% duplicates allowed
    'null_threshold': 0.8,  # 80% non-null required for critical fields
    'validation_threshold': 0.9  # 90% of records should pass validation
}

# File Processing Configuration
FILE_CONFIG = {
    'supported_formats': ['.xlsx', '.xls', '.csv'],
    'encoding': 'utf-8',
    'max_file_size_mb': 100,
    'temp_directory': 'logs'
}

# Database Schema Configuration
DATABASE_SCHEMA = {
    'staging_table': {
        'name': 'staging_candidates',
        'columns': [
            'stable_id VARCHAR(16) PRIMARY KEY',
            'candidate_name TEXT',
            'state VARCHAR(50)',
            'office TEXT',
            'party VARCHAR(50)',
            'district VARCHAR(100)',
            'county VARCHAR(100)',
            'city VARCHAR(100)',
            'address TEXT',
            'zip_code VARCHAR(10)',
            'phone VARCHAR(15)',
            'email VARCHAR(255)',
            'website VARCHAR(255)',
            'filing_date DATE',
            'election_date DATE',
            'election_type VARCHAR(100)',
            'source_file VARCHAR(255)',
            'data_version VARCHAR(50)',
            'action_type VARCHAR(20)',
            'created_at TIMESTAMP',
            'updated_at TIMESTAMP'
        ]
    },
    'production_table': {
        'name': 'filings',
        'columns': [
            'stable_id VARCHAR(16) PRIMARY KEY',
            'candidate_name TEXT',
            'state VARCHAR(50)',
            'office TEXT',
            'party VARCHAR(50)',
            'district VARCHAR(100)',
            'county VARCHAR(100)',
            'city VARCHAR(100)',
            'address TEXT',
            'zip_code VARCHAR(10)',
            'phone VARCHAR(15)',
            'email VARCHAR(255)',
            'website VARCHAR(255)',
            'filing_date DATE',
            'election_date DATE',
            'election_type VARCHAR(100)',
            'source_file VARCHAR(255)',
            'data_version VARCHAR(50)',
            'created_at TIMESTAMP',
            'updated_at TIMESTAMP'
        ]
    }
} 