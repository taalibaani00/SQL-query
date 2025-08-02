#!/usr/bin/env python3
"""
Extract Slow Game Failures - Extract only slow failure records from the original CSV
Creates a new CSV with only the rows that match slow game failure criteria
"""

import pandas as pd
import os
from datetime import datetime

def extract_slow_failure_records(input_file, output_file=None, min_time_threshold=5):
    """
    Extract records that match slow game failure criteria and save to new CSV
    
    Criteria:
    - reason = 'matchmaking-failed'
    - game_id has exactly 2 users
    - minimum failure time >= 5 seconds
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path for the output CSV file (optional)
        min_time_threshold (int): Minimum time threshold in seconds (default: 5)
    """
    try:
        print(f"📖 Reading CSV file: {input_file}")
        df = pd.read_csv(input_file)
        print(f"📊 Total records in file: {len(df):,}")
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        print(f"📊 Matchmaking failure records: {len(matchmaking_failed):,}")
        
        # Find games with exactly 2 users (fishy cases)
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        fishy_cases = game_user_counts[game_user_counts['user_count'] == 2]
        fishy_game_ids = fishy_cases['game_id'].tolist()
        
        print(f"🔍 Games with 2 users (fishy cases): {len(fishy_cases):,}")
        
        # Get details for fishy cases
        fishy_details = matchmaking_failed[matchmaking_failed['game_id'].isin(fishy_game_ids)]
        
        if len(fishy_details) == 0:
            print("❌ No fishy cases found in the data.")
            return None
        
        # Calculate timing for each record
        print("⏱️ Calculating timing for each record...")
        fishy_details = fishy_details.copy()
        fishy_details['created_at_dt'] = pd.to_datetime(fishy_details['created_at'])
        fishy_details['updated_at_dt'] = pd.to_datetime(fishy_details['updated_at'])
        fishy_details['time_diff_seconds'] = (fishy_details['updated_at_dt'] - fishy_details['created_at_dt']).dt.total_seconds()
        
        # Find slow failure game IDs (games where minimum time >= threshold)
        slow_failure_game_ids = []
        
        print(f"🐌 Identifying games with minimum failure time >= {min_time_threshold} seconds...")
        for game_id in fishy_game_ids:
            game_data = fishy_details[fishy_details['game_id'] == game_id]
            if len(game_data) >= 2:
                min_time = game_data['time_diff_seconds'].min()
                if min_time >= min_time_threshold:
                    slow_failure_game_ids.append(game_id)
        
        if not slow_failure_game_ids:
            print(f"❌ No slow failure games found (≥{min_time_threshold}s).")
            return None
        
        print(f"🎯 Found {len(slow_failure_game_ids):,} slow failure games")
        
        # Extract all records for slow failure games from the ORIGINAL dataset
        print("📋 Extracting all records for slow failure games from original dataset...")
        slow_failure_records = df[df['game_id'].isin(slow_failure_game_ids)]
        
        print(f"✅ Total records to extract: {len(slow_failure_records):,}")
        
        # Generate output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"slow_game_failures_{timestamp}.csv"
        
        # Save to CSV
        print(f"💾 Saving to: {output_file}")
        slow_failure_records.to_csv(output_file, index=False)
        
        # Print summary
        print(f"\n{'='*80}")
        print("🎯 SLOW GAME FAILURE EXTRACTION SUMMARY")
        print(f"{'='*80}")
        print(f"📊 Input file: {input_file}")
        print(f"📊 Output file: {output_file}")
        print(f"📊 Original total records: {len(df):,}")
        print(f"📊 Matchmaking failures: {len(matchmaking_failed):,}")
        print(f"📊 Games with 2 users: {len(fishy_cases):,}")
        print(f"📊 Slow failure games (≥{min_time_threshold}s): {len(slow_failure_game_ids):,}")
        print(f"📊 Records extracted: {len(slow_failure_records):,}")
        print(f"📊 Extraction rate: {(len(slow_failure_records)/len(df)*100):.2f}% of original data")
        
        # Show some example game IDs
        print(f"\n🎮 SAMPLE SLOW FAILURE GAME IDs (first 10):")
        for i, game_id in enumerate(slow_failure_game_ids[:10], 1):
            print(f"{i:2d}. {game_id}")
        
        if len(slow_failure_game_ids) > 10:
            print(f"    ... and {len(slow_failure_game_ids) - 10} more")
        
        # Show column information
        print(f"\n📋 OUTPUT CSV COLUMNS:")
        print(f"Columns: {list(slow_failure_records.columns)}")
        
        print(f"\n✅ Successfully created: {output_file}")
        print(f"{'='*80}")
        
        return slow_failure_records
        
    except Exception as e:
        print(f"❌ Error extracting slow failure records: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """
    Main function to extract slow failure records
    """
    print("🐌 Extract Slow Game Failures - CSV Record Extractor")
    print("=" * 60)
    
    # Input file path - update this to your CSV file location
    input_file = "/Users/karansunkariya/Downloads/query_result_2025-07-28T15_52_18.838209Z.csv"
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        print("Please update the input_file variable in the main() function.")
        return
    
    # Extract slow failure records
    print("🔍 Extracting slow failure records...")
    result = extract_slow_failure_records(input_file)
    
    if result is not None:
        print(f"\n✅ Extraction complete!")
        print(f"💡 TIP: You can now use the generated CSV file for further analysis")
    else:
        print("❌ Extraction failed or no records found.")


if __name__ == "__main__":
    main() 