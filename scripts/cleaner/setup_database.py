#!/usr/bin/env python3
"""
Database Setup Helper

This script helps you set up your Supabase database connection.
It will guide you through creating a .env file with your credentials.
"""

import os
from pathlib import Path

def create_env_file():
    """Create a .env file with Supabase credentials."""
    env_file = Path('.env')
    
    if env_file.exists():
        print("⚠️  .env file already exists!")
        response = input("Do you want to overwrite it? (y/N): ").strip().lower()
        if response != 'y':
            print("Setup cancelled.")
            return
    
    print("\n🔧 Setting up Supabase Database Connection")
    print("=" * 50)
    print("\nYou'll need to get these values from your Supabase dashboard:")
    print("1. Go to https://supabase.com/dashboard")
    print("2. Select your project")
    print("3. Go to Settings > Database")
    print("4. Copy the connection details\n")
    
    # Get credentials from user
    host = input("Enter your Supabase host (e.g., abc123.supabase.co): ").strip()
    if not host:
        print("❌ Host is required!")
        return
    
    port = input("Enter database port (default: 5432): ").strip() or "5432"
    database = input("Enter database name (default: postgres): ").strip() or "postgres"
    user = input("Enter database user (default: postgres): ").strip() or "postgres"
    password = input("Enter database password: ").strip()
    if not password:
        print("❌ Password is required!")
        return
    
    # Optional API keys
    anon_key = input("Enter anon key (optional): ").strip()
    service_key = input("Enter service role key (optional): ").strip()
    
    # Create .env content
    env_content = f"""# Supabase Database Connection
SUPABASE_HOST={host}
SUPABASE_PORT={port}
SUPABASE_DATABASE={database}
SUPABASE_USER={user}
SUPABASE_PASSWORD={password}
"""
    
    if anon_key:
        env_content += f"SUPABASE_ANON_KEY={anon_key}\n"
    if service_key:
        env_content += f"SUPABASE_SERVICE_ROLE_KEY={service_key}\n"
    
    # Write .env file
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print(f"\n✅ Created {env_file}")
        print("🔒 Your credentials are now saved in .env file")
        print("⚠️  Make sure .env is in your .gitignore to keep credentials secure!")
        
        # Test if we can read the file
        if os.path.exists('.env'):
            print("\n🧪 Testing environment variables...")
            from dotenv import load_dotenv
            load_dotenv()
            
            # Check if variables are loaded
            if os.getenv('SUPABASE_HOST'):
                print("✅ Environment variables loaded successfully!")
                print("\n🚀 You can now run: python test_db_connection.py")
            else:
                print("❌ Environment variables not loaded. You may need to install python-dotenv:")
                print("   pip install python-dotenv")
        
    except Exception as e:
        print(f"❌ Error creating .env file: {e}")

def main():
    """Main setup function."""
    print("🚀 Supabase Database Setup")
    print("=" * 30)
    
    # Check if .env already exists
    if Path('.env').exists():
        print("📁 .env file found!")
        response = input("Do you want to reconfigure? (y/N): ").strip().lower()
        if response != 'y':
            print("Setup cancelled.")
            return
    
    create_env_file()

if __name__ == "__main__":
    main()

