#!/usr/bin/env python3
"""
Generic Matchmaking Analyzer - A Python script to analyze matchmaking failures
for both 2-player and 6-player games using max_seats from playing_tables

UPDATED VERSION: Now uses max_seats column to definitively determine game type
- 2-Player games: max_seats = 2
- 6-Player games: max_seats = 6

Expected CSV columns:
id, is_shadowed, reason, updated_at, created_at, created_by, updated_by, 
game_id, table_id, user_id, max_seats

SQL Query:
SELECT tm.*, pt.max_seats
FROM table_game_user_map tm 
LEFT JOIN playing_tables pt ON pt.table_id = tm.table_id 
WHERE tm.created_at >= NOW() - INTERVAL 16 HOUR;

Supports comprehensive analysis including timing, user patterns, and executive reporting
"""

import csv
import os
import sys
from pathlib import Path

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def detect_game_type_and_status(player_count, max_seats):
    """
    Detect game type and failure status based on player count and max_seats
    
    Args:
        player_count (int): Number of players associated with a game_id
        max_seats (int): Maximum seats for the game (2 or 6)
        
    Returns:
        tuple: (game_type, status, category)
    """
    if max_seats == 2:
        if player_count == 1:
            return "2-PLAYER", "NORMAL", "Single player timeout (expected)"
        elif player_count == 2:
            return "2-PLAYER", "FISHY", "Both players present but failed"
        else:  
            return "2-PLAYER", "SUSPICIOUS", f"System bug - {player_count} players in 2P game"
    
    elif max_seats == 6:
        if player_count <= 2:
            return "6-PLAYER", "NORMAL", f"Insufficient players ({player_count}/6) - need 3+ to play"
        elif player_count in [3, 4, 5, 6]:
            return "6-PLAYER", "FISHY", f"Sufficient players ({player_count}/6) but failed"
        else:  
            return "6-PLAYER", "SUSPICIOUS", f"System bug - {player_count} players in 6P game"
    
    else:
        # Unknown game type
        return "UNKNOWN", "SUSPICIOUS", f"Unknown max_seats: {max_seats}"


def print_analysis_definitions():
    """
    Print clear definitions of analysis categories
    """
    print(f"\n📋 ANALYSIS CATEGORIES DEFINITIONS:")
    print("=" * 70)
    
    print(f"🟢 NORMAL Cases:")
    print(f"├── 2-Player Games: Games with insufficient players (1/2 players) - Expected timeout")
    print(f"└── 6-Player Games: Games with insufficient players (≤2/6 players) - Expected timeout")
    print()
    
    print(f"🔴 FISHY Cases:")
    print(f"├── 2-Player Games: Games with sufficient players (2/2 players) but still failed")
    print(f"└── 6-Player Games: Games with sufficient players (3-6/6 players) but still failed")
    print()
    
    print(f"🚨 SUSPICIOUS Cases:")
    print(f"├── 2-Player Games: Games with excess players (>2 players) - System bug")
    print(f"└── 6-Player Games: Games with excess players (>6 players) - System bug")
    print()
    
    print(f"💡 KEY INSIGHT:")
    print(f"├── NORMAL = Games with insufficient players (EXPECTED)")
    print(f"├── FISHY = Games with sufficient players but failed (INVESTIGATE)")
    print(f"└── SUSPICIOUS = Games with excess players (CRITICAL BUG)")
    print()
    print("🎯 Focus debugging efforts on FISHY cases - these indicate race conditions,")
    print("   deadlocks, or other system issues preventing game start despite sufficient players.")
    print()
    print("📝 NOTE: 6-Player games need minimum 3 players to start (not 6).")


def read_csv_with_pandas(file_path):
    """
    Read CSV file using pandas library
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please install it with: pip install pandas")
        return None
    
    try:
        
        df = pd.read_csv(file_path)
        
        print(f"Reading CSV file with pandas: {file_path}")
        print(f"Shape: {df.shape} (rows, columns)")
        print(f"Columns: {list(df.columns)}")
        print("-" * 50)
        
        print("Data types:")
        print(df.dtypes)
        print("\nFirst 5 rows:")
        print(df.head())
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading CSV file with pandas: {e}")
        return None


def analyze_generic_matchmaking_patterns(file_path):
    """
    Analyze matchmaking failures for both 2-player and 6-player games
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        df = pd.read_csv(file_path)
        
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        print(f"\n{'='*80}")
        print("GENERIC MATCHMAKING FAILURE ANALYSIS (2P + 6P GAMES)")
        print(f"{'='*80}")
        
        game_user_counts = matchmaking_failed.groupby('game_id').agg({
            'user_id': 'count',
            'max_seats': 'first' 
        }).reset_index()
        game_user_counts.columns = ['game_id', 'user_count', 'max_seats']
        
        game_analysis = []
        for _, row in game_user_counts.iterrows():
            game_id = row['game_id']
            player_count = row['user_count']
            max_seats = row['max_seats']
            game_type, status, description = detect_game_type_and_status(player_count, max_seats)
            
            game_analysis.append({
                'game_id': game_id,
                'player_count': player_count,
                'max_seats': max_seats,
                'game_type': game_type,
                'status': status,
                'description': description
            })
        
        analysis_df = pd.DataFrame(game_analysis)
        
        print(f"📊 OVERALL SUMMARY:")
        print(f"├── Total Records: {len(df):,}")
        print(f"├── Matchmaking Failures: {len(matchmaking_failed):,}")
        print(f"├── Unique Games: {len(game_user_counts):,}")
        print(f"└── Failure Rate: {(len(matchmaking_failed)/len(df)*100):.1f}%")
        
        # Max seats distribution (shows intended game types)
        max_seats_distribution = analysis_df['max_seats'].value_counts().sort_index()
        print(f"\n🎮 INTENDED GAME TYPE DISTRIBUTION (by max_seats):")
        print("-" * 60)
        for max_seats, count in max_seats_distribution.items():
            percentage = (count / len(analysis_df) * 100)
            game_type_name = f"{max_seats}-Player" if max_seats in [2, 6] else f"Unknown ({max_seats} seats)"
            print(f"├── {game_type_name}: {count:,} games ({percentage:.1f}%)")
        
        type_distribution = analysis_df['game_type'].value_counts()
        status_distribution = analysis_df['status'].value_counts()
        
        print(f"\n📊 ANALYSIS RESULT DISTRIBUTION:")
        print("-" * 60)
        for game_type, count in type_distribution.items():
            percentage = (count / len(analysis_df) * 100)
            print(f"├── {game_type}: {count:,} games ({percentage:.1f}%)")
        
        print(f"\n🚨 FAILURE STATUS DISTRIBUTION:")
        print("-" * 60)
        for status, count in status_distribution.items():
            percentage = (count / len(analysis_df) * 100)
            print(f"├── {status}: {count:,} games ({percentage:.1f}%)")
        
        print_analysis_definitions()
        
        print(f"\n📋 DETAILED BREAKDOWN:")
        print("-" * 80)
        
        pivot_table = pd.crosstab(analysis_df['game_type'], analysis_df['status'], margins=True)
        print(pivot_table)
        
        timing_2p_result = analyze_2player_games(matchmaking_failed, analysis_df)
        timing_6p_result = analyze_6player_games(matchmaking_failed, analysis_df)
        analyze_suspicious_games(matchmaking_failed, analysis_df)
        
        analyze_top_failing_users_generic(matchmaking_failed, analysis_df)
        
        generate_comparative_analysis(matchmaking_failed, analysis_df)
        
        # Collect timing results for executive summary
        timing_results = {}
        if timing_2p_result:
            timing_results['2-PLAYER'] = timing_2p_result
        if timing_6p_result:
            timing_results['6-PLAYER'] = timing_6p_result
        
        generate_executive_summary_generic(df, matchmaking_failed, analysis_df, timing_results)
        
        return analysis_df
        
    except Exception as e:
        print(f"Error analyzing generic matchmaking patterns: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_2player_games(matchmaking_failed, analysis_df):
    """
    Detailed analysis of 2-player games
    Returns timing analysis result for executive summary
    """
    print(f"\n{'='*80}")
    print("🎯 2-PLAYER GAMES ANALYSIS")
    print(f"{'='*80}")
    
    games_2p = analysis_df[analysis_df['game_type'] == '2-PLAYER']
    
    if len(games_2p) == 0:
        print("No 2-player games found in the dataset.")
        return
    
    print(f"Total 2-player games: {len(games_2p):,}")
    
    status_2p = games_2p['status'].value_counts()
    print(f"\n2-Player Game Status:")
    for status, count in status_2p.items():
        percentage = (count / len(games_2p) * 100)
        print(f"├── {status}: {count:,} games ({percentage:.1f}%)")
    
    games_2p_ids = games_2p['game_id'].tolist()
    games_2p_details = matchmaking_failed[matchmaking_failed['game_id'].isin(games_2p_ids)]
    
    fishy_2p = games_2p[games_2p['status'] == 'FISHY']
    timing_2p_result = None
    if len(fishy_2p) > 0:
        print(f"\n⏱️ TIMING ANALYSIS FOR 2P FISHY GAMES ({len(fishy_2p):,} games):")
        print("-" * 60)
        timing_2p_result = analyze_timing_patterns(games_2p_details, fishy_2p['game_id'].tolist(), "2-PLAYER")
    
    created_by_2p = games_2p_details['created_by'].value_counts()
    print(f"\n👤 CREATED_BY PATTERNS (2-Player Games):")
    print("-" * 50)
    total_2p_records = len(games_2p_details)
    for created_by, count in created_by_2p.items():
        percentage = (count / total_2p_records * 100)
        print(f"├── {created_by}: {count:,} ({percentage:.1f}%)")
    
    return timing_2p_result


def analyze_6player_games(matchmaking_failed, analysis_df):
    """
    Detailed analysis of 6-player games (UPDATED: 3+ players can play)
    Returns timing analysis result for executive summary
    """
    print(f"\n{'='*80}")
    print("🎲 6-PLAYER GAMES ANALYSIS (3+ players needed to play)")  
    print(f"{'='*80}")
    
    games_6p = analysis_df[analysis_df['game_type'] == '6-PLAYER']
    
    if len(games_6p) == 0:
        print("No 6-player games found in the dataset.")
        return
    
    print(f"Total 6-player games: {len(games_6p):,}")
    
    status_6p = games_6p['status'].value_counts()
    print(f"\n6-Player Game Status:")
    for status, count in status_6p.items():
        percentage = (count / len(games_6p) * 100)
        print(f"├── {status}: {count:,} games ({percentage:.1f}%)")
    
    player_count_6p = games_6p['player_count'].value_counts().sort_index()
    print(f"\n📊 Player Count Distribution (6P Games - 3+ needed to play):")
    print("-" * 60)
    for player_count, game_count in player_count_6p.items():
        percentage = (game_count / len(games_6p) * 100)
        
        if player_count <= 2:
            status_desc = "NORMAL - Insufficient players"
        elif player_count in [3, 4, 5, 6]:
            status_desc = "FISHY - Sufficient players but failed"
        else:
            status_desc = "SUSPICIOUS - Too many players"
            
        print(f"├── {player_count} players: {game_count:,} games ({percentage:.1f}%) - {status_desc}")
    
    games_6p_ids = games_6p['game_id'].tolist()
    games_6p_details = matchmaking_failed[matchmaking_failed['game_id'].isin(games_6p_ids)]
    
    # Timing analysis for 6P FISHY games
    fishy_6p = games_6p[games_6p['status'] == 'FISHY']
    timing_6p_result = None
    if len(fishy_6p) > 0:
        print(f"\n⏱️ TIMING ANALYSIS FOR 6P FISHY GAMES ({len(fishy_6p):,} games):")
        print("-" * 60)
        timing_6p_result = analyze_timing_patterns(games_6p_details, fishy_6p['game_id'].tolist(), "6-PLAYER")
    
    created_by_6p = games_6p_details['created_by'].value_counts()
    print(f"\n👤 CREATED_BY PATTERNS (6-Player Games):")
    print("-" * 50)
    total_6p_records = len(games_6p_details)
    for created_by, count in created_by_6p.items():
        percentage = (count / total_6p_records * 100)
        print(f"├── {created_by}: {count:,} ({percentage:.1f}%)")
    
    return timing_6p_result


def analyze_suspicious_games(matchmaking_failed, analysis_df):
    """
    Detailed analysis of suspicious games (too many players for game type)
    """
    print(f"\n{'='*80}")
    print("🚨 SUSPICIOUS GAMES ANALYSIS (System Bugs)")
    print(f"{'='*80}")
    
    games_suspicious = analysis_df[analysis_df['status'] == 'SUSPICIOUS']
    
    if len(games_suspicious) == 0:
        print("✅ No suspicious games found - Good system health!")
        return
    
    print(f"🚨 CRITICAL: {len(games_suspicious):,} games with system bugs detected!")
    print("This indicates serious issues allowing wrong number of players per game.")
    
    suspicious_by_type = games_suspicious.groupby('game_type').size()
    print(f"\n📊 Suspicious Games by Type:")
    print("-" * 60)
    for game_type, count in suspicious_by_type.items():
        print(f"├── {game_type}: {count:,} games")
    
    print(f"\n🔍 DETAILED BREAKDOWN:")
    print("-" * 80)
    print(f"{'Game Type':<12} {'Max Seats':<10} {'Actual Players':<15} {'Count':<8} {'Examples'}")
    print("-" * 80)
    
    suspicious_breakdown = games_suspicious.groupby(['game_type', 'max_seats', 'player_count']).size().reset_index(name='count')
    for _, row in suspicious_breakdown.iterrows():
        game_type = row['game_type']
        max_seats = row['max_seats']
        player_count = row['player_count']
        count = row['count']
        
        examples = games_suspicious[
            (games_suspicious['game_type'] == game_type) & 
            (games_suspicious['max_seats'] == max_seats) & 
            (games_suspicious['player_count'] == player_count)
        ]['game_id'].head(2).tolist()
        examples_str = ", ".join(examples[:2])
        if len(examples) > 2:
            examples_str += "..."
        
        print(f"{game_type:<12} {max_seats:<10} {player_count:<15} {count:<8} {examples_str}")
    
    print(f"\n💥 IMPACT ASSESSMENT:")
    print("-" * 50)
    total_suspicious_records = len(matchmaking_failed[matchmaking_failed['game_id'].isin(games_suspicious['game_id'])])
    print(f"├── Suspicious games: {len(games_suspicious):,}")
    print(f"├── Affected failure records: {total_suspicious_records:,}")
    print(f"└── System integrity: COMPROMISED - Immediate fixes required")


def analyze_timing_patterns(game_details, game_ids, game_type):
    """
    Analyze timing patterns for specific games
    Returns timing distribution for use in executive summary
    """
    timing_data = game_details[game_details['game_id'].isin(game_ids)].copy()
    
    if len(timing_data) == 0:
        print("No timing data available for analysis.")
        return None
    
    # Convert datetime columns
    date_format = "%B %d, %Y, %I:%M:%S.%f %p"
    timing_data.loc[:, 'created_at_dt'] = pd.to_datetime(timing_data['created_at'], format=date_format, errors='coerce')
    timing_data.loc[:, 'updated_at_dt'] = pd.to_datetime(timing_data['updated_at'], format=date_format, errors='coerce')
    timing_data.loc[:, 'time_diff_seconds'] = (timing_data['updated_at_dt'] - timing_data['created_at_dt']).dt.total_seconds()
    
    timing_analysis = []
    for game_id in game_ids:
        game_data = timing_data[timing_data['game_id'] == game_id]
        if len(game_data) > 0:
            min_time = game_data['time_diff_seconds'].min()
            max_time = game_data['time_diff_seconds'].max()
            avg_time = game_data['time_diff_seconds'].mean()
            
            timing_analysis.append({
                'game_id': game_id,
                'min_time': min_time,
                'max_time': max_time,
                'avg_time': avg_time,
                'player_count': len(game_data)
            })
    
    if not timing_analysis:
        print("No valid timing data found.")
        return None
    
    timing_df = pd.DataFrame(timing_analysis)
    
    def categorize_time(min_time):
        if min_time < 2:
            return "< 2 seconds"
        elif min_time < 5:
            return "2-5 seconds"
        else:
            return ">= 5 seconds"
    
    timing_df['time_category'] = timing_df['min_time'].apply(categorize_time)
    
    time_distribution = timing_df['time_category'].value_counts()
    
    print(f"Distribution of minimum wait times before failure ({game_type}):")
    print("-" * 50)
    
    total_games = len(timing_df)
    for category, count in time_distribution.items():
        percentage = (count / total_games) * 100
        print(f"├── {category}: {count:,} games ({percentage:.1f}%)")
    
    print(f"\nTiming Statistics ({game_type}):")
    print(f"├── Average min time: {timing_df['min_time'].mean():.2f} seconds")
    print(f"├── Median min time: {timing_df['min_time'].median():.2f} seconds")
    print(f"├── Fastest failure: {timing_df['min_time'].min():.2f} seconds")
    print(f"└── Slowest failure: {timing_df['min_time'].max():.2f} seconds")
    
    # Return timing distribution for executive summary
    return {
        'game_type': game_type,
        'distribution': time_distribution,
        'total_games': total_games,
        'stats': {
            'avg_time': timing_df['min_time'].mean(),
            'median_time': timing_df['min_time'].median(),
            'min_time': timing_df['min_time'].min(),
            'max_time': timing_df['min_time'].max()
        }
    }


def analyze_top_failing_users_generic(matchmaking_failed, analysis_df):
    """
    Analyze top users with most matchmaking failures across all game types
    """
    print(f"\n{'='*80}")
    print("👥 TOP FAILING USERS ANALYSIS (All Game Types)")
    print(f"{'='*80}")
    
    user_failure_counts = matchmaking_failed['user_id'].value_counts()
    top_10_users = user_failure_counts.head(10)
    
    total_failures = len(matchmaking_failed)
    print(f"Total matchmaking failures: {total_failures:,}")
    print(f"Unique users with failures: {len(user_failure_counts):,}")
    
    print(f"\n📊 TOP 10 USERS BY FAILURE COUNT:")
    print("-" * 90)
    print(f"{'Rank':<4} {'User ID':<26} {'Total Failures':<15} {'% of All':<10} {'Game Types'}")
    print("-" * 90)
    
    for i, (user_id, failure_count) in enumerate(top_10_users.items(), 1):
        percentage = (failure_count / total_failures) * 100
        
        user_failures = matchmaking_failed[matchmaking_failed['user_id'] == user_id]
        user_game_ids = user_failures['game_id'].unique()
        
        user_game_types = []
        for game_id in user_game_ids:
            game_info = analysis_df[analysis_df['game_id'] == game_id]
            if len(game_info) > 0:
                game_type = game_info.iloc[0]['game_type']
                if game_type not in user_game_types:
                    user_game_types.append(game_type)
        
        game_types_str = ", ".join(user_game_types)
        print(f"{i:<4} {user_id:<26} {failure_count:<15} {percentage:<9.1f}% {game_types_str}")


def generate_comparative_analysis(matchmaking_failed, analysis_df):
    """
    Generate comparative analysis between 2-player and 6-player games           
    """
    print(f"\n{'='*80}")
    print("⚖️  COMPARATIVE ANALYSIS: 2-PLAYER vs 6-PLAYER GAMES")
    print(f"{'='*80}")
    
    games_2p = analysis_df[analysis_df['game_type'] == '2-PLAYER']
    games_6p = analysis_df[analysis_df['game_type'] == '6-PLAYER']
    
    if len(games_2p) == 0 and len(games_6p) == 0:
        print("No 2-player or 6-player games found for comparison.")
        return
    
    print(f"📊 GAME VOLUME COMPARISON:")
    print("-" * 60)
    total_games = len(analysis_df)
    
    if len(games_2p) > 0:
        games_2p_percentage = (len(games_2p) / total_games * 100)
        print(f"├── 2-Player Games: {len(games_2p):,} ({games_2p_percentage:.1f}%)")
    
    if len(games_6p) > 0:
        games_6p_percentage = (len(games_6p) / total_games * 100)
        print(f"├── 6-Player Games: {len(games_6p):,} ({games_6p_percentage:.1f}%)")
    
    print(f"\n🚨 FAILURE PATTERN COMPARISON:")
    print("-" * 60)
    
    if len(games_2p) > 0:
        fishy_2p = len(games_2p[games_2p['status'] == 'FISHY'])
        normal_2p = len(games_2p[games_2p['status'] == 'NORMAL'])
        suspicious_2p = len(games_2p[games_2p['status'] == 'SUSPICIOUS'])
        
        fishy_2p_rate = (fishy_2p / len(games_2p) * 100) if len(games_2p) > 0 else 0
        
        print(f"2-Player Games ({len(games_2p):,} total):")
        print(f"├── Normal (1 player): {normal_2p:,} ({(normal_2p/len(games_2p)*100):.1f}%)")
        print(f"├── Fishy (2 players): {fishy_2p:,} ({fishy_2p_rate:.1f}%) ⚠️")
        print(f"└── Suspicious (>2): {suspicious_2p:,} ({(suspicious_2p/len(games_2p)*100):.1f}%)")
        print()
    
    if len(games_6p) > 0:
        fishy_6p = len(games_6p[games_6p['status'] == 'FISHY'])
        normal_6p = len(games_6p[games_6p['status'] == 'NORMAL'])
        suspicious_6p = len(games_6p[games_6p['status'] == 'SUSPICIOUS'])
        
        fishy_6p_rate = (fishy_6p / len(games_6p) * 100) if len(games_6p) > 0 else 0
        
        print(f"6-Player Games ({len(games_6p):,} total):")
        print(f"├── Normal (≤2 players): {normal_6p:,} ({(normal_6p/len(games_6p)*100):.1f}%)")
        print(f"├── Fishy (3-6 players): {fishy_6p:,} ({fishy_6p_rate:.1f}%) ⚠️")
        print(f"└── Suspicious (>6): {suspicious_6p:,} ({(suspicious_6p/len(games_6p)*100):.1f}%)")
    
    print(f"\n💡 KEY INSIGHTS:")
    print("-" * 50)
    
    if len(games_2p) > 0 and len(games_6p) > 0:
        if fishy_2p_rate > fishy_6p_rate:
            difference = fishy_2p_rate - fishy_6p_rate
            print(f"├── 2-Player games are MORE problematic ({difference:.1f}% higher fishy rate)")
            print(f"│   └── Focus debugging efforts on 2P matchmaking logic")
        elif fishy_6p_rate > fishy_2p_rate:
            difference = fishy_6p_rate - fishy_2p_rate
            print(f"├── 6-Player games are MORE problematic ({difference:.1f}% higher fishy rate)")
            print(f"│   └── Focus debugging efforts on 6P matchmaking logic")
        else:
            print(f"├── Both game types have similar fishy rates (~{fishy_2p_rate:.1f}%)")
            print(f"│   └── System-wide matchmaking issues affecting both types")
    
    elif len(games_2p) > 0:
        print(f"├── Only 2-player games found in dataset")
        print(f"│   └── Fishy rate: {fishy_2p_rate:.1f}%")
    
    elif len(games_6p) > 0:
        print(f"├── Only 6-player games found in dataset")
        print(f"│   └── Fishy rate: {fishy_6p_rate:.1f}%")
    
    if len(games_6p) > 0:
        print(f"\n📈 6-PLAYER GAME FILL RATE ANALYSIS (3+ players can play):")
        print("-" * 60)
        player_dist_6p = games_6p['player_count'].value_counts().sort_index()
        
        print("Player count distribution for 6P games:")
        for player_count, game_count in player_dist_6p.items():
            percentage = (game_count / len(games_6p) * 100)
            fill_rate = (player_count / 6 * 100)
            
            if player_count <= 2:
                play_status = "Can't play"
            else:
                play_status = "Can play"
                
            print(f"├── {player_count} players: {game_count:,} games ({percentage:.1f}%) - {fill_rate:.0f}% filled - {play_status}")
        
        playable_games = games_6p[games_6p['player_count'] >= 3]
        playable_rate = (len(playable_games) / len(games_6p) * 100)
        print(f"└── Games with 3+ players (playable): {len(playable_games):,} ({playable_rate:.1f}%)")


def generate_executive_summary_generic(original_df, matchmaking_failed, analysis_df, timing_results=None):
    """
    Generate executive summary for generic matchmaking analysis
    """
    print(f"\n{'='*80}")
    print("📊 MATCHMAKING SYSTEM ANALYSIS - EXECUTIVE SUMMARY")
    print(f"{'='*80}")
    
    total_games = len(analysis_df)
    total_failures = len(matchmaking_failed)
    total_records = len(original_df)
    
    failure_rate = (total_failures / total_records * 100) if total_records > 0 else 0
    
    type_counts = analysis_df['game_type'].value_counts()
    status_counts = analysis_df['status'].value_counts()
    
    fishy_count = status_counts.get('FISHY', 0)
    fishy_percentage = (fishy_count / total_games * 100) if total_games > 0 else 0
    system_status = "CRITICAL ISSUES IDENTIFIED" if fishy_percentage > 30 else "MODERATE ISSUES" if fishy_percentage > 15 else "SYSTEM HEALTHY"
    
    print(f"\n🎯 EXECUTIVE OVERVIEW")
    print(f"├── System Status: {system_status}")
    print(f"├── Total Failures Analyzed: {total_failures:,} matchmaking attempts")
    print(f"├── Failure Rate: {failure_rate:.1f}% of all sampled attempts")
    print(f"└── Unique Games Affected: {total_games:,} game instances")
    
    print(f"\n📈 FAILURE CATEGORIZATION")
    
    normal_count = status_counts.get('NORMAL', 0)
    fishy_count = status_counts.get('FISHY', 0)
    suspicious_count = status_counts.get('SUSPICIOUS', 0)
    
    normal_percentage = (normal_count / total_games * 100) if total_games > 0 else 0
    fishy_percentage = (fishy_count / total_games * 100) if total_games > 0 else 0
    
    print(f"├── 🟢 NORMAL FAILURES ({normal_percentage:.1f}% - {normal_count:,} games)")
    
    normal_2p = len(analysis_df[(analysis_df['game_type'] == '2-PLAYER') & (analysis_df['status'] == 'NORMAL')])
    normal_6p = len(analysis_df[(analysis_df['game_type'] == '6-PLAYER') & (analysis_df['status'] == 'NORMAL')])
    
    if normal_2p > 0:
        print(f"│   ├── Definition: Insufficient players for game completion")
        print(f"│   ├── Expected Behavior: Timeout when not enough players join")
        print(f"│   ├── 2-Player Games (1/2): {normal_2p:,} cases ({(normal_2p/normal_count*100):.1f}%)" if normal_count > 0 else "│   ├── 2-Player Games: 0 cases")
        print(f"│   ├── 6-Player Games (≤2/6): {normal_6p:,} cases ({(normal_6p/normal_count*100):.1f}%)" if normal_count > 0 else "│   ├── 6-Player Games: 0 cases")
    else:
        print(f"│   ├── Definition: Insufficient players for game completion") 
        print(f"│   ├── Expected Behavior: Timeout when not enough players join")
        print(f"│   ├── 2-Player Games (1/2): 0 cases (0.0%)")
        print(f"│   └── 6-Player Games (≤2/6): {normal_6p:,} cases (100.0%)" if normal_count > 0 else "│   └── 6-Player Games: 0 cases")
    
    if fishy_count > 0:
        print(f"│")
        print(f"└── 🔴 FISHY FAILURES ({fishy_percentage:.1f}% - {fishy_count:,} games)")
        print(f"    ├── Definition: Sufficient players present but game still failed")
        
        fishy_2p = len(analysis_df[(analysis_df['game_type'] == '2-PLAYER') & (analysis_df['status'] == 'FISHY')])
        fishy_6p = len(analysis_df[(analysis_df['game_type'] == '6-PLAYER') & (analysis_df['status'] == 'FISHY')])
        
        print(f"    └── Game Type Breakdown:")
        if fishy_2p > 0:
            print(f"        ├── 2-Player (2/2): {fishy_2p:,} games ({(fishy_2p/fishy_count*100):.1f}%)")
        if fishy_6p > 0:
            print(f"        └── 6-Player (3-6/6): {fishy_6p:,} games ({(fishy_6p/fishy_count*100):.1f}%)")
    
    if suspicious_count > 0:
        print(f"\n🚨 SUSPICIOUS CASES: {suspicious_count:,} games")
        print(f"└── Definition: System bugs allowing wrong player counts")
    
    # Add timing analysis section
    if timing_results:
        print(f"\n⏱️ TIMING ANALYSIS FOR FISHY FAILURES")
        
        # Calculate overall timing statistics
        total_fishy_games = 0
        combined_distribution = {"< 2 seconds": 0, "2-5 seconds": 0, ">= 5 seconds": 0}
        
        for game_type, timing_data in timing_results.items():
            distribution = timing_data['distribution']
            total_games = timing_data['total_games']
            total_fishy_games += total_games
            
            for category, count in distribution.items():
                combined_distribution[category] += count
        
        # Calculate quick failures vs slow failures
        quick_failures = combined_distribution["< 2 seconds"] + combined_distribution["2-5 seconds"]
        slow_failures = combined_distribution[">= 5 seconds"]
        
        quick_percentage = (quick_failures / total_fishy_games * 100) if total_fishy_games > 0 else 0
        slow_percentage = (slow_failures / total_fishy_games * 100) if total_fishy_games > 0 else 0
        
        print(f"├── 🟡 QUICK FAILURES ({quick_percentage:.1f}% - {quick_failures:,} games)")
        print(f"│   ├── Definition: Games failing within 5 seconds of player join")
        print(f"│   ├── Indicates: Race conditions, server overload, or logic errors")
        
        if combined_distribution["< 2 seconds"] > 0:
            ultra_quick_pct = (combined_distribution["< 2 seconds"] / quick_failures * 100) if quick_failures > 0 else 0
            print(f"│   ├── Ultra-quick (< 2s): {combined_distribution['< 2 seconds']:,} games ({ultra_quick_pct:.1f}%)")
        
        if combined_distribution["2-5 seconds"] > 0:
            medium_quick_pct = (combined_distribution["2-5 seconds"] / quick_failures * 100) if quick_failures > 0 else 0
            print(f"│   └── Medium-quick (2-5s): {combined_distribution['2-5 seconds']:,} games ({medium_quick_pct:.1f}%)")
        
        print(f"│")
        print(f"└── 🔵 SLOW FAILURES ({slow_percentage:.1f}% - {slow_failures:,} games)")
        print(f"    ├── Definition: Games failing after 5+ seconds (expected timeout)")
        print(f"    ├── Indicates: Proper matchmaking timeout behavior")
        print(f"    └── Game Type Breakdown:")
        
        for game_type, timing_data in timing_results.items():
            distribution = timing_data['distribution']
            slow_count = distribution.get(">= 5 seconds", 0)
            if slow_count > 0:
                slow_game_pct = (slow_count / slow_failures * 100) if slow_failures > 0 else 0
                print(f"        ├── {game_type}: {slow_count:,} games ({slow_game_pct:.1f}%)")


def main():
    """
    Main function to demonstrate generic matchmaking analysis
    """
    print("Generic Matchmaking Analyzer - Multi-Game-Type Analysis Tool")
    print("=" * 70)
    
    file_path = "query_result_2025-07-30T11_45_43.43219Z.csv"  
    
    if PANDAS_AVAILABLE:
        df = read_csv_with_pandas(file_path)
        
        if df is not None:
            print(f"\n{'='*70}")
            print("GENERIC MATCHMAKING FAILURE ANALYSIS")
            print(f"{'='*70}")
            
            matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
            
            print(f"Total records: {len(df):,}")
            print(f"Matchmaking failed records: {len(matchmaking_failed):,}")
            print(f"Percentage of failures: {(len(matchmaking_failed)/len(df)*100):.1f}%")
            print("-" * 70)
            
            if len(matchmaking_failed) > 0:
                analysis_result = analyze_generic_matchmaking_patterns(file_path)
                
                if analysis_result is not None:
                    print(f"\n✅ Analysis completed successfully!")
                    print(f"📁 Results available in analysis_result DataFrame")
            else:
                print("No matchmaking failures found in the dataset.")
                    
    else:
        print("pandas is not available. Please activate your virtual environment.")
        print("Run: source .venv/bin/activate")


if __name__ == "__main__":
    main() 