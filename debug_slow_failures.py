#!/usr/bin/env python3
"""
Debug Slow Failures - A Python script to extract and analyze slow critical failures
Focuses on matchmaking failures with ≥5s timing for debugging purposes
"""

import csv
import os
import sys
from pathlib import Path
from datetime import datetime

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def extract_slow_critical_failures(file_path, output_file=None, min_time_threshold=5):
    """
    Extract and save critical failures with ≥5s timing to a separate CSV file for debugging
    
    Args:
        file_path (str): Path to the input CSV file
        output_file (str): Path for the output CSV file (optional)
        min_time_threshold (int): Minimum time threshold in seconds (default: 5)
        
    Returns:
        pandas.DataFrame: DataFrame containing slow critical failures
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please install it with: pip install pandas")
        return None
    
    try:
        # Read CSV file
        print(f"📖 Reading CSV file: {file_path}")
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        print(f"📊 Total matchmaking failures: {len(matchmaking_failed):,}")
        
        # Get fishy cases (2 users per game_id)
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        fishy_cases = game_user_counts[game_user_counts['user_count'] == 2]
        fishy_game_ids = fishy_cases['game_id'].tolist()
        
        print(f"🔍 Critical failures (2 players): {len(fishy_cases):,} games")
        
        # Get details for fishy cases
        fishy_details = matchmaking_failed[matchmaking_failed['game_id'].isin(fishy_game_ids)]
        
        if len(fishy_details) == 0:
            print("❌ No critical failures found in the data.")
            return None
        
        # Calculate timing
        fishy_details = fishy_details.copy()
        fishy_details['created_at_dt'] = pd.to_datetime(fishy_details['created_at'])
        fishy_details['updated_at_dt'] = pd.to_datetime(fishy_details['updated_at'])
        fishy_details['time_diff_seconds'] = (fishy_details['updated_at_dt'] - fishy_details['created_at_dt']).dt.total_seconds()
        
        # Group by game_id and find minimum time for each game
        slow_failures = []
        
        for game_id in fishy_game_ids:
            game_data = fishy_details[fishy_details['game_id'] == game_id]
            if len(game_data) >= 2:
                min_time = game_data['time_diff_seconds'].min()
                
                # Only include games with ≥threshold minimum failure time
                if min_time >= min_time_threshold:
                    # Add timing analysis columns to the game data
                    game_data_enhanced = game_data.copy()
                    game_data_enhanced['min_failure_time'] = min_time
                    game_data_enhanced['max_failure_time'] = game_data['time_diff_seconds'].max()
                    game_data_enhanced['avg_failure_time'] = game_data['time_diff_seconds'].mean()
                    
                    # Add pattern analysis
                    creators = game_data['created_by'].unique()
                    game_data_enhanced['pattern_type'] = 'Mixed' if len(creators) > 1 else 'Same'
                    game_data_enhanced['creators_involved'] = ', '.join(creators)
                    
                    # Add hour analysis
                    game_data_enhanced['failure_hour'] = game_data_enhanced['created_at_dt'].dt.hour
                    game_data_enhanced['failure_day'] = game_data_enhanced['created_at_dt'].dt.day_name()
                    
                    slow_failures.append(game_data_enhanced)
        
        if not slow_failures:
            print(f"❌ No slow critical failures (≥{min_time_threshold}s) found in the data.")
            return None
        
        # Combine all slow failures
        slow_failures_df = pd.concat(slow_failures, ignore_index=True)
        
        # Sort by min_failure_time descending to see slowest first
        slow_failures_df = slow_failures_df.sort_values('min_failure_time', ascending=False)
        
        # Generate output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"slow_critical_failures_{timestamp}.csv"
        
        # Print detailed analysis
        print_slow_failure_analysis(slow_failures_df, output_file, min_time_threshold)
        
        return slow_failures_df
        
    except Exception as e:
        print(f"❌ Error extracting slow critical failures: {e}")
        return None


def print_slow_failure_analysis(df, output_file, min_time_threshold):
    """
    Print detailed analysis of slow failures
    
    Args:
        df (pandas.DataFrame): DataFrame containing slow failures
        output_file (str): Output CSV filename
        min_time_threshold (int): Minimum time threshold used
    """
    total_slow_games = len(df['game_id'].unique())
    total_slow_records = len(df)
    
    print(f"\n{'='*80}")
    print(f"🐌 SLOW CRITICAL FAILURES ANALYSIS (≥{min_time_threshold}s)")
    print(f"{'='*80}")
    
    print(f"\n📊 SUMMARY:")
    print(f"├── Total slow failure games: {total_slow_games:,}")
    print(f"├── Total records extracted: {total_slow_records:,}")
    print(f"├── Output file: {output_file}")
    print(f"├── Average failure time: {df['min_failure_time'].mean():.2f} seconds")
    print(f"├── Median failure time: {df['min_failure_time'].median():.2f} seconds")
    print(f"├── Slowest failure: {df['min_failure_time'].max():.2f} seconds")
    print(f"└── Fastest slow failure: {df['min_failure_time'].min():.2f} seconds")
    
    # Pattern analysis
    pattern_counts = df.groupby('game_id')['pattern_type'].first().value_counts()
    print(f"\n🔍 PATTERN BREAKDOWN:")
    for pattern, count in pattern_counts.items():
        percentage = (count / total_slow_games * 100)
        print(f"├── {pattern} patterns: {count:,} games ({percentage:.1f}%)")
    
    # Creator analysis
    creator_counts = df['created_by'].value_counts()
    print(f"\n👥 CREATOR BREAKDOWN:")
    for creator, count in creator_counts.items():
        percentage = (count / total_slow_records * 100)
        print(f"├── {creator}: {count:,} records ({percentage:.1f}%)")
    
    # Timing distribution
    timing_ranges = [
        ("5-10s", (5, 10)),
        ("10-20s", (10, 20)),
        ("20-30s", (20, 30.1)),  # Include 30 seconds in this range
        ("30-60s", (30.1, 60)),  # Start after 30 seconds
        ("60s+", (60, float('inf')))
    ]
    
    print(f"\n⏱️ TIMING DISTRIBUTION:")
    for range_name, (min_time, max_time) in timing_ranges:
        if max_time == float('inf'):
            count = len(df[df['min_failure_time'] >= min_time]['game_id'].unique())
        else:
            count = len(df[
                (df['min_failure_time'] >= min_time) & 
                (df['min_failure_time'] < max_time)
            ]['game_id'].unique())
        
        percentage = (count / total_slow_games * 100) if total_slow_games > 0 else 0
        print(f"├── {range_name}: {count:,} games ({percentage:.1f}%)")
    
    # Show top 15 slowest cases
    print(f"\n🔥 TOP 15 SLOWEST FAILURES:")
    print("-" * 80)
    
    top_slow_games = df.groupby('game_id').agg({
        'min_failure_time': 'first',
        'pattern_type': 'first',
        'creators_involved': 'first',
        'table_id': 'first',
        'failure_hour': 'first',
        'failure_day': 'first'
    }).sort_values('min_failure_time', ascending=False).head(15)
    
    for i, (game_id, row) in enumerate(top_slow_games.iterrows(), 1):
        print(f"{i:2d}. Game {game_id}: {row['min_failure_time']:.1f}s")
        print(f"    Pattern: {row['pattern_type']} | Creators: {row['creators_involved']}")
        print(f"    Table: {row['table_id']} | Time: {row['failure_day']} {row['failure_hour']:02d}:00")
        print()
    
    # Table ID analysis
    table_counts = df['table_id'].value_counts().head(10)
    if len(table_counts) > 0:
        print(f"\n🏓 TOP PROBLEMATIC TABLES:")
        for table_id, count in table_counts.items():
            percentage = (count / total_slow_records * 100)
            print(f"├── Table {table_id}: {count:,} failures ({percentage:.1f}%)")
    
    print(f"\n🎯 DEBUGGING RECOMMENDATIONS:")
    print("├── 1. Focus on 60s+ cases first (likely full timeout scenarios)")
    print("├── 2. Check Mixed pattern cases for integration issues")
    print("├── 3. Look for common table_ids with multiple failures")
    print("├── 4. Analyze peak hour patterns for load-related issues")
    print("├── 5. Cross-reference timestamps with system logs")
    print("├── 6. Check if specific days have more issues")
    print("└── 7. Investigate tables with highest failure rates")
    
    print(f"\n{'='*80}")


def analyze_specific_game(df, game_id):
    """
    Analyze a specific game in detail
    
    Args:
        df (pandas.DataFrame): DataFrame containing slow failures
        game_id (str): Game ID to analyze
    """
    game_data = df[df['game_id'] == game_id]
    
    if len(game_data) == 0:
        print(f"❌ Game {game_id} not found in slow failures data.")
        return
    
    print(f"\n{'='*60}")
    print(f"🔍 DETAILED ANALYSIS: Game {game_id}")
    print(f"{'='*60}")
    
    print(f"📊 GAME OVERVIEW:")
    print(f"├── Players involved: {len(game_data)}")
    print(f"├── Pattern type: {game_data.iloc[0]['pattern_type']}")
    print(f"├── Creators: {game_data.iloc[0]['creators_involved']}")
    print(f"├── Table ID: {game_data.iloc[0]['table_id']}")
    print(f"├── Failure time: {game_data.iloc[0]['failure_day']} {game_data.iloc[0]['failure_hour']:02d}:00")
    print(f"├── Min failure time: {game_data.iloc[0]['min_failure_time']:.2f}s")
    print(f"├── Max failure time: {game_data.iloc[0]['max_failure_time']:.2f}s")
    print(f"└── Avg failure time: {game_data.iloc[0]['avg_failure_time']:.2f}s")
    
    print(f"\n👥 PLAYER DETAILS:")
    for i, (_, player) in enumerate(game_data.iterrows(), 1):
        print(f"{i}. User {player['user_id']}:")
        print(f"   ├── Created by: {player['created_by']}")
        print(f"   ├── Wait time: {player['time_diff_seconds']:.2f}s")
        print(f"   ├── Created at: {player['created_at']}")
        print(f"   └── Updated at: {player['updated_at']}")
        print()


def analyze_tables_over_60s(df):
    """
    Analyze and display table IDs where failure time is greater than 60 seconds
    
    Args:
        df (pandas.DataFrame): DataFrame containing slow failures
    """
    if df is None or len(df) == 0:
        print("❌ No data available for analysis.")
        return
    
    # Debug: Show actual timing statistics first
    print(f"\n{'='*80}")
    print(f"🔍 DEBUGGING TIMING ANALYSIS")
    print(f"{'='*80}")
    
    timing_stats = df.groupby('game_id')['min_failure_time'].first()
    print(f"\n📊 ACTUAL TIMING STATISTICS:")
    print(f"├── Total games analyzed: {len(timing_stats):,}")
    print(f"├── Minimum failure time: {timing_stats.min():.2f}s")
    print(f"├── Maximum failure time: {timing_stats.max():.2f}s")
    print(f"├── Average failure time: {timing_stats.mean():.2f}s")
    print(f"├── Median failure time: {timing_stats.median():.2f}s")
    print(f"├── 75th percentile: {timing_stats.quantile(0.75):.2f}s")
    print(f"├── 90th percentile: {timing_stats.quantile(0.90):.2f}s")
    print(f"├── 95th percentile: {timing_stats.quantile(0.95):.2f}s")
    print(f"└── 99th percentile: {timing_stats.quantile(0.99):.2f}s")
    
    # Show count of games in different ranges
    ranges_debug = [
        ("5-10s", 5, 10),
        ("10-20s", 10, 20),
        ("20-30s", 20, 30.1),  # Include 30 seconds in this range
        ("30-40s", 30.1, 40),  # Start after 30 seconds
        ("40-50s", 40, 50),
        ("50-60s", 50, 60),
        ("60-70s", 60, 70),
        ("70s+", 70, float('inf'))
    ]
    
    print(f"\n⏱️ DETAILED TIMING BREAKDOWN:")
    for range_name, min_time, max_time in ranges_debug:
        if max_time == float('inf'):
            count = len(timing_stats[timing_stats >= min_time])
        else:
            count = len(timing_stats[(timing_stats >= min_time) & (timing_stats < max_time)])
        
        percentage = (count / len(timing_stats) * 100) if len(timing_stats) > 0 else 0
        print(f"├── {range_name}: {count:,} games ({percentage:.1f}%)")
    
    # Filter for failures > 60 seconds
    print(f"\n🚨 CRITICAL FAILURES > 60 SECONDS - TABLE ANALYSIS")
    critical_failures = df[df['min_failure_time'] > 60]
    print(f"🚨 CRITICAL FAILURES > 60 SECONDS - TABLE ANALYSIS")
    
    if len(critical_failures) == 0:
        print("✅ No failures found with timing > 60 seconds.")
        
        # Since no >60s failures, let's analyze the slowest failures (50-60s range)
        print(f"\n🔥 ANALYZING SLOWEST FAILURES (50-60s RANGE) INSTEAD:")
        slowest_failures = df[df['min_failure_time'] >= 50]
        
        if len(slowest_failures) == 0:
            print("❌ No failures found with timing ≥ 50 seconds either.")
            return
        
        analyze_critical_table_failures(slowest_failures, "50+ seconds")
        return
    
    analyze_critical_table_failures(critical_failures, "60+ seconds")


def analyze_critical_table_failures(critical_failures, threshold_description):
    """
    Analyze critical table failures for a given threshold
    
    Args:
        critical_failures (pandas.DataFrame): DataFrame containing critical failures
        threshold_description (str): Description of the threshold (e.g., "60+ seconds")
    """
    # Get unique games
    critical_games = critical_failures.groupby('game_id').agg({
        'min_failure_time': 'first',
        'table_id': 'first',
        'pattern_type': 'first',
        'creators_involved': 'first',
        'failure_hour': 'first',
        'failure_day': 'first'
    }).sort_values('min_failure_time', ascending=False)
    
    total_critical_games = len(critical_games)
    
    print(f"\n📊 OVERVIEW ({threshold_description}):")
    print(f"├── Total games: {total_critical_games:,}")
    print(f"├── Slowest failure: {critical_games['min_failure_time'].max():.1f}s")
    print(f"├── Average time: {critical_games['min_failure_time'].mean():.1f}s")
    print(f"└── Median time: {critical_games['min_failure_time'].median():.1f}s")
    
    # Table ID analysis
    table_analysis = critical_games['table_id'].value_counts()
    
    print(f"\n🏓 TABLE IDs WITH {threshold_description.upper()} FAILURES:")
    print("-" * 60)
    
    for i, (table_id, count) in enumerate(table_analysis.items(), 1):
        percentage = (count / total_critical_games * 100)
        
        # Get timing stats for this table
        table_games = critical_games[critical_games['table_id'] == table_id]
        avg_time = table_games['min_failure_time'].mean()
        max_time = table_games['min_failure_time'].max()
        
        print(f"{i:2d}. Table {table_id}:")
        print(f"    ├── Failures: {count:,} games ({percentage:.1f}%)")
        print(f"    ├── Avg time: {avg_time:.1f}s")
        print(f"    ├── Max time: {max_time:.1f}s")
        
        # Show pattern breakdown for this table
        table_patterns = table_games['pattern_type'].value_counts()
        pattern_info = ", ".join([f"{pattern}: {cnt}" for pattern, cnt in table_patterns.items()])
        print(f"    └── Patterns: {pattern_info}")
        print()
    
    # Show detailed breakdown of each critical failure
    print(f"\n🔥 DETAILED BREAKDOWN - ALL GAMES ({threshold_description}):")
    print("-" * 80)
    
    for i, (game_id, row) in enumerate(critical_games.iterrows(), 1):
        print(f"{i:2d}. Game {game_id} | Table {row['table_id']} | {row['min_failure_time']:.1f}s")
        print(f"    Pattern: {row['pattern_type']} | Creators: {row['creators_involved']}")
        print(f"    Time: {row['failure_day']} {row['failure_hour']:02d}:00")
        print()
    
    print(f"\n🎯 RECOMMENDATIONS FOR {threshold_description.upper()} FAILURES:")
    print("├── 1. PRIORITY: Investigate these tables immediately")
    print("├── 2. Check server resources during failure times")
    print("├── 3. Look for database locks or connection issues")
    print("├── 4. Verify table-specific configurations")
    print("├── 5. Check for memory leaks or resource exhaustion")
    print("└── 6. Consider table rotation if needed")
    
    print(f"\n{'='*80}")


def analyze_games_20_30s(df):
    """
    Analyze and display game IDs where failure time is between 20-30 seconds (inclusive of 30s)
    
    Args:
        df (pandas.DataFrame): DataFrame containing slow failures
    """
    if df is None or len(df) == 0:
        print("❌ No data available for analysis.")
        return
    
    # Filter for failures between 20-30 seconds (inclusive of 30s)
    target_failures = df[(df['min_failure_time'] >= 20) & (df['min_failure_time'] <= 30)]
    
    if len(target_failures) == 0:
        print("❌ No failures found in the 20-30s timing range.")
        return
    
    print(f"\n{'='*80}")
    print(f"🎯 GAME IDs WITH 20-30s FAILURES (INCLUDING 30s)")
    print(f"{'='*80}")
    
    # Get unique games in 20-30s range
    target_games = target_failures.groupby('game_id').agg({
        'min_failure_time': 'first',
        'table_id': 'first',
        'pattern_type': 'first',
        'creators_involved': 'first',
        'failure_hour': 'first',
        'failure_day': 'first'
    }).sort_values('min_failure_time', ascending=False)
    
    total_games = len(target_games)
    
    print(f"\n📊 20-30s RANGE OVERVIEW:")
    print(f"├── Total games: {total_games:,}")
    print(f"├── Slowest in range: {target_games['min_failure_time'].max():.1f}s")
    print(f"├── Fastest in range: {target_games['min_failure_time'].min():.1f}s")
    print(f"├── Average time: {target_games['min_failure_time'].mean():.1f}s")
    print(f"└── Median time: {target_games['min_failure_time'].median():.1f}s")
    
    # Count games with exactly 30s timing
    exactly_30s = target_games[target_games['min_failure_time'] == 30.0]
    print(f"\n⏰ GAMES WITH EXACTLY 30s TIMING: {len(exactly_30s):,}")
    
    # Show all games in 20-30s range sorted by timing (slowest first)
    print(f"\n🎮 ALL GAME IDs IN 20-30s RANGE (SORTED BY TIMING):")
    print("-" * 80)
    
    for i, (game_id, row) in enumerate(target_games.iterrows(), 1):
        timing_marker = " ⭐ EXACTLY 30s" if row['min_failure_time'] == 30.0 else ""
        print(f"{i:2d}. Game ID: {game_id}{timing_marker}")
        print(f"    ├── Failure time: {row['min_failure_time']:.1f}s")
        print(f"    ├── Table ID: {row['table_id']}")
        print(f"    ├── Pattern: {row['pattern_type']}")
        print(f"    ├── Creators: {row['creators_involved']}")
        print(f"    └── Time: {row['failure_day']} {row['failure_hour']:02d}:00")
        print()
    
    # Summary of all game IDs
    all_game_ids = target_games.index.tolist()
    print(f"\n📝 SUMMARY - ALL GAME IDs IN 20-30s RANGE:")
    print(f"├── Total games: {len(all_game_ids)}")
    print(f"├── Game IDs (comma-separated):")
    
    # Print game IDs in chunks of 5 for readability
    chunk_size = 5
    for i in range(0, len(all_game_ids), chunk_size):
        chunk = all_game_ids[i:i+chunk_size]
        print(f"│   {', '.join(chunk)}")
    
    # Show exactly 30s games separately if any exist
    if len(exactly_30s) > 0:
        exactly_30s_ids = exactly_30s.index.tolist()
        print(f"\n⭐ GAMES WITH EXACTLY 30s TIMING:")
        print(f"├── Count: {len(exactly_30s_ids)}")
        print(f"├── Game IDs: {', '.join(exactly_30s_ids)}")
    
    print(f"\n🎯 RECOMMENDATIONS FOR 20-30s GAME FAILURES:")
    print("├── 1. Focus on games with exactly 30s timing (likely timeout threshold)")
    print("├── 2. Check if 30s is a configured timeout value")
    print("├── 3. Investigate why games are hitting this specific timing")
    print("├── 4. Look for patterns in 30s failures vs others in range")
    print("├── 5. Check system configuration for 30s timeouts")
    print("└── 6. Monitor if this is a hard timeout or processing delay")
    
    print(f"\n{'='*80}")


def analyze_games_30_40s(df):
    """
    Analyze and display game IDs where failure time is between 30-40 seconds (excluding 30s)
    
    Args:
        df (pandas.DataFrame): DataFrame containing slow failures
    """
    if df is None or len(df) == 0:
        print("❌ No data available for analysis.")
        return
    
    # Filter for failures between 30-40 seconds (excluding 30s since it's now in 20-30s range)
    target_failures = df[(df['min_failure_time'] > 30) & (df['min_failure_time'] < 40)]
    
    if len(target_failures) == 0:
        print("❌ No failures found in the 30-40s timing range (excluding 30s).")
        return
    
    print(f"\n{'='*80}")
    print(f"🎯 GAME IDs WITH 30-40s FAILURES (EXCLUDING 30s)")
    print(f"{'='*80}")
    
    # Get unique games in 30-40s range
    target_games = target_failures.groupby('game_id').agg({
        'min_failure_time': 'first',
        'table_id': 'first',
        'pattern_type': 'first',
        'creators_involved': 'first',
        'failure_hour': 'first',
        'failure_day': 'first'
    }).sort_values('min_failure_time', ascending=False)
    
    total_games = len(target_games)
    
    print(f"\n📊 30-40s RANGE OVERVIEW (EXCLUDING 30s):")
    print(f"├── Total games: {total_games:,}")
    if total_games > 0:
        print(f"├── Slowest in range: {target_games['min_failure_time'].max():.1f}s")
        print(f"├── Fastest in range: {target_games['min_failure_time'].min():.1f}s")
        print(f"├── Average time: {target_games['min_failure_time'].mean():.1f}s")
        print(f"└── Median time: {target_games['min_failure_time'].median():.1f}s")
        
        # Show all games in 30-40s range sorted by timing (slowest first)
        print(f"\n🎮 ALL GAME IDs IN 30-40s RANGE (SORTED BY TIMING):")
        print("-" * 80)
        
        for i, (game_id, row) in enumerate(target_games.iterrows(), 1):
            print(f"{i:2d}. Game ID: {game_id}")
            print(f"    ├── Failure time: {row['min_failure_time']:.1f}s")
            print(f"    ├── Table ID: {row['table_id']}")
            print(f"    ├── Pattern: {row['pattern_type']}")
            print(f"    ├── Creators: {row['creators_involved']}")
            print(f"    └── Time: {row['failure_day']} {row['failure_hour']:02d}:00")
            print()
        
        # Summary of all game IDs
        all_game_ids = target_games.index.tolist()
        print(f"\n📝 SUMMARY - ALL GAME IDs IN 30-40s RANGE:")
        print(f"├── Total games: {len(all_game_ids)}")
        print(f"├── Game IDs (comma-separated):")
        
        # Print game IDs in chunks of 5 for readability
        chunk_size = 5
        for i in range(0, len(all_game_ids), chunk_size):
            chunk = all_game_ids[i:i+chunk_size]
            print(f"│   {', '.join(chunk)}")
    
    print(f"\n{'='*80}")


def main():
    """
    Main function to run slow failure analysis
    """
    print("🐌 Debug Slow Failures - Matchmaking Analysis Tool")
    print("=" * 60)
    
    # Default file path - update this to your CSV file location
    file_path = "query_result_2025-07-30T10_21_16.087214Z.csv"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        print("Please update the file_path variable in the main() function.")
        return
    
    if PANDAS_AVAILABLE:
        # Extract slow critical failures
        print("🔍 Extracting slow critical failures...")
        df = extract_slow_critical_failures(file_path)
        
        if df is not None:
            print(f"\n✅ Analysis complete! Check the generated CSV file for detailed data.")
            
            # Analyze tables with > 60s failures
            # analyze_tables_over_60s(df)
            
            # Analyze games with 20-30s failures specifically
            # analyze_games_20_30s(df)
            
            # Analyze games with 30-40s failures specifically
            # analyze_games_30_40s(df)
            
            # Optional: Analyze specific games
            print(f"\n💡 TIP: You can analyze specific games by calling:")
            print(f"analyze_specific_game(df, 'your_game_id')")
            analyze_specific_game(df, '6848f838c8c6de0a1a7931f2')
            
            # Show some example game IDs for analysis
            if len(df) > 0:
                sample_games = df['game_id'].unique()[:3]
                print(f"\nExample game IDs to analyze:")
                for game_id in sample_games:
                    print(f"- {game_id}")
        else:
            print("❌ No slow failures found or error occurred.")
                    
    else:
        print("❌ pandas is not available. Please install it with: pip install pandas")


if __name__ == "__main__":
    main() 