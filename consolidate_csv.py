#!/usr/bin/env python3
"""
Script to consolidate CSV files and add appversion column.
Combines data from the original CSV and the new CSV with appversion information.
"""

import csv
import sys
from collections import defaultdict

def consolidate_csv_files():
    """
    Consolidate CSV files and add appversion column.
    """
    
    # File paths
    original_csv = "slow_game_failures_20250702_213030.csv"
    new_csv_path = "/Users/karansunkariya/Downloads/ed7d94e9-779a-4a8e-b3b7-fdca15a1fc7b.csv"
    output_csv = "consolidated_game_failures.csv"
    
    # Dictionary to store appversion data by (gameid, uid) key
    appversion_data = {}
    
    print("Reading appversion data from new CSV...")
    try:
        with open(new_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use gameid and uid as key
                key = (row.get('gameid', ''), row.get('uid', ''))
                appversion_data[key] = row.get('appversion', '')
        
        print(f"Loaded {len(appversion_data)} appversion records")
        
    except FileNotFoundError:
        print(f"Warning: Could not find new CSV file at {new_csv_path}")
        print("Proceeding without appversion data...")
    except Exception as e:
        print(f"Error reading new CSV: {e}")
        print("Proceeding without appversion data...")
    
    print("Processing original CSV and adding appversion column...")
    
    # Process original CSV and add appversion column
    with open(original_csv, 'r', encoding='utf-8') as infile, \
         open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add appversion to fieldnames
        fieldnames = list(reader.fieldnames) + ['appversion']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        processed_count = 0
        appversion_matches = 0
        
        for row in reader:
            # Look up appversion using game_id and user_id
            key = (row.get('game_id', ''), row.get('user_id', ''))
            appversion = appversion_data.get(key, '')
            
            if appversion:
                appversion_matches += 1
            
            # Add appversion to row
            row['appversion'] = appversion
            writer.writerow(row)
            
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} rows...")
    
    print(f"\\nConsolidation complete!")
    print(f"Total rows processed: {processed_count}")
    print(f"Rows with appversion data: {appversion_matches}")
    print(f"Output file: {output_csv}")
    
    # Generate summary
    print(f"\\nSummary:")
    print(f"- Original CSV: {original_csv}")
    print(f"- Appversion CSV: {new_csv_path}")
    print(f"- Consolidated CSV: {output_csv}")
    print(f"- Added appversion column with {appversion_matches} matches out of {processed_count} total rows")

if __name__ == "__main__":
    consolidate_csv_files() 