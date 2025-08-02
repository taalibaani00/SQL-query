#!/usr/bin/env python3
"""
Script to generate Athena SQL query from CSV data.
Extracts unique game_id and user_id values from CSV and creates a query like:
select gameid, uid, appversion from mongo_rummy.registrations_vw 
where gameid in ('value1', 'value2', ...) and uid in ('value1', 'value2', ...);
"""

import pandas as pd
import sys

def generate_athena_query(csv_file: str, output_file: str = None) -> str:
    """
    Generate Athena SQL query from CSV data.
    
    Args:
        csv_file: Path to the CSV file
        output_file: Optional path to save the query to a file
        
    Returns:
        The generated SQL query string
    """
    
    try:
        # Read the CSV file
        print(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} records")
        
        # Check if required columns exist
        if 'game_id' not in df.columns:
            print("Error: Column 'game_id' not found in CSV")
            return None
        if 'user_id' not in df.columns:
            print("Error: Column 'user_id' not found in CSV")
            return None
            
        # Extract unique values
        unique_game_ids = df['game_id'].dropna().unique().tolist()
        unique_user_ids = df['user_id'].dropna().unique().tolist()
        
        print(f"Found {len(unique_game_ids)} unique game_ids")
        print(f"Found {len(unique_user_ids)} unique user_ids")
        
        # Format game_ids for SQL IN clause
        game_ids_formatted = "', '".join(unique_game_ids)
        game_ids_sql = f"('{game_ids_formatted}')"
        
        # Format user_ids for SQL IN clause  
        user_ids_formatted = "', '".join(unique_user_ids)
        user_ids_sql = f"('{user_ids_formatted}')"
        
        # Generate the SQL query
        sql_query = f"""select gameid, uid, appversion 
from mongo_rummy.registrations_vw 
where gameid in {game_ids_sql} 
and uid in {user_ids_sql};"""
        
        # Print the query
        print("\n" + "="*80)
        print("GENERATED ATHENA SQL QUERY:")
        print("="*80)
        print(sql_query)
        print("="*80)
        
        # Save to file if specified
        if output_file:
            with open(output_file, 'w') as f:
                f.write(sql_query)
            print(f"\nQuery saved to: {output_file}")
        
        # Print some statistics
        print(f"\nQuery Statistics:")
        print(f"- Number of game_ids in IN clause: {len(unique_game_ids)}")
        print(f"- Number of user_ids in IN clause: {len(unique_user_ids)}")
        print(f"- Estimated max result rows: {len(unique_game_ids) * len(unique_user_ids)}")
        
        # Show first few values for verification
        print(f"\nFirst 5 game_ids: {unique_game_ids[:5]}")
        print(f"First 5 user_ids: {unique_user_ids[:5]}")
        
        return sql_query
        
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None

def generate_compact_query(csv_file: str) -> str:
    """
    Generate a more compact version of the query (single line).
    """
    try:
        df = pd.read_csv(csv_file)
        unique_game_ids = df['game_id'].dropna().unique().tolist()
        unique_user_ids = df['user_id'].dropna().unique().tolist()
        
        game_ids_formatted = "', '".join(unique_game_ids)
        user_ids_formatted = "', '".join(unique_user_ids)
        
        compact_query = f"select gameid, uid, appversion from mongo_rummy.registrations_vw where gameid in ('{game_ids_formatted}') and uid in ('{user_ids_formatted}');"
        
        return compact_query
    except Exception as e:
        print(f"Error generating compact query: {str(e)}")
        return None

def main():
    """Main function to generate the Athena query."""
    
    csv_file = "slow_game_failures_20250702_213030.csv"
    output_file = "athena_query.sql"
    
    print("Generating Athena SQL Query from CSV data...")
    print("-" * 50)
    
    # Generate the formatted query
    query = generate_athena_query(csv_file, output_file)
    
    if query:
        print("\n" + "="*80)
        print("COMPACT VERSION (SINGLE LINE):")
        print("="*80)
        compact = generate_compact_query(csv_file)
        if compact:
            print(compact)
        
        print("\n✅ Query generation completed successfully!")
        print(f"You can now copy and paste the query above into Amazon Athena.")
    else:
        print("\n❌ Failed to generate query.")

if __name__ == "__main__":
    main() 