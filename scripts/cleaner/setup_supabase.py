#!/usr/bin/env python3
"""
Quick Supabase Setup

This script helps you create a .env file with your Supabase credentials.
"""

import os
from pathlib import Path

def main():
    print("🔧 Setting up Supabase Connection")
    print("=" * 40)
    print("\nYour Supabase Project Details:")
    print("• Project: Candidate Filings")
    print("• Project ID: bnvpsoppbufldabquoec")
    print("• Host: aws-0-us-east-2.pooler.supabase.com")
    print("• Port: 5432 (session pooler)")
    print("• Database: postgres")
    print("• User: postgres.bnvpsoppbufldabquoec")
    
    print("\n⚠️  You need to get your database password from:")
    print("1. Go to https://supabase.com/dashboard")
    print("2. Select 'Candidate Filings' project")
    print("3. Go to Settings > Database")
    print("4. Click 'Reset your database password'")
    print("5. Copy the new password\n")
    
    password = input("Enter your Supabase database password: ").strip()
    
    if not password:
        print("❌ Password is required!")
        return
    
    # Create .env content
    env_content = f"""# Supabase Database Connection
# Project: Candidate Filings (bnvpsoppbufldabquoec)

# Session pooler (recommended for reliability)
SUPABASE_HOST=aws-0-us-east-2.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres.bnvpsoppbufldabquoec
SUPABASE_PASSWORD={password}

# API Keys (optional)
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJudnBzb3BwYnVmbGRhYnF1b2VjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE3Njc2MjAsImV4cCI6MjA2NzM0MzYyMH0.wXRNsbFLS90_mkqpx7fko0o6jPOvM_ZBde_GqXdOfb8
SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJudnBzb3BwYnVmbGRhYnF1b2VjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTc2NzYyMCwiZXhwIjoyMDY3MzQzNjIwfQ.4phfSXHEpuA35nGbpxNBuXmIpcypmviFisakLIWRDAE
"""
    
    # Write .env file
    env_file = Path('.env')
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print(f"\n✅ Created {env_file}")
        print("🔒 Your credentials are now saved")
        print("⚠️  Make sure .env is in your .gitignore!")
        
        print("\n🚀 Now you can test the connection:")
        print("   python test_db_connection.py")
        
    except Exception as e:
        print(f"❌ Error creating .env file: {e}")

if __name__ == "__main__":
    main()
