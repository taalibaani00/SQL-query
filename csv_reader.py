#!/usr/bin/env python3
"""
CSV Reader - A Python script to read data from CSV files
Demonstrates multiple approaches to reading CSV data
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
        # Read CSV with pandas (automatically detects separator)
        df = pd.read_csv(file_path)
        
        print(f"Reading CSV file with pandas: {file_path}")
        print(f"Shape: {df.shape} (rows, columns)")
        print(f"Columns: {list(df.columns)}")
        print("-" * 50)
        
        # Display basic info
        print("Data types:")
        print(df.dtypes)
        print("\nFirst 5 rows:")
        print(df.head())
        
        # Display basic statistics for numeric columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        if len(numeric_columns) > 0:
            print("\nBasic statistics for numeric columns:")
            print(df[numeric_columns].describe())
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading CSV file with pandas: {e}")
        return None


def analyze_game_id_patterns(file_path):
    """
    Analyze matchmaking failures by game_id to find anomalous patterns
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        print(f"\n{'='*70}")
        print("GAME_ID PATTERN ANALYSIS FOR MATCHMAKING FAILURES")
        print(f"{'='*70}")
        
        # Group by game_id and count users per game
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        
        # Separate normal vs anomalous cases
        normal_cases = game_user_counts[game_user_counts['user_count'] == 1]
        anomalous_cases = game_user_counts[game_user_counts['user_count'] == 2]
        suspicious_cases = game_user_counts[game_user_counts['user_count'] > 2]
        
        print(f"Total unique game_ids with failures: {len(game_user_counts)}")
        print(f"Games with 1 user (NORMAL): {len(normal_cases)}")
        print(f"Games with 2 users (FISHY): {len(anomalous_cases)}")
        print("-" * 70)
        
        # Show normal cases summary
        print(f"\n📊 NORMAL CASES (1 user per failed game_id): {len(normal_cases)}")
        print("These make sense - one player joined, waited 30s, no second player came")
        
        # Analyze normal cases by created_by
        if len(normal_cases) > 0:
            print(f"\n🔍 NORMAL CASES DISTRIBUTION BY CREATED_BY:")
            print("-" * 50)
            
            # Get the created_by for normal cases
            normal_game_ids = normal_cases['game_id'].tolist()
            normal_details = matchmaking_failed[matchmaking_failed['game_id'].isin(normal_game_ids)]
            
            # Group by created_by
            normal_created_by_counts = normal_details['created_by'].value_counts()
            
            total_normal = len(normal_cases)
            for created_by, count in normal_created_by_counts.items():
                percentage = (count / total_normal) * 100
                print(f"{created_by}: {count} cases ({percentage:.1f}%)")
            
            print(f"\nNormal cases breakdown:")
            print(f"- Total normal cases: {total_normal}")
            print(f"- rummy-registration normal: {normal_created_by_counts.get('rummy-registration', 0)}")
            print(f"- new-game-start normal: {normal_created_by_counts.get('new-game-start', 0)}")
        
        # Show anomalous cases in detail
        if len(anomalous_cases) > 0:
            print(f"\n🚨 FISHY CASES (2 users per failed game_id): {len(anomalous_cases)}")
            print("These are suspicious - why did game fail if 2 players were present?")
            print("-" * 50)
            
            for _, row in anomalous_cases.iterrows():
                game_id = row['game_id']
                # print(f"\nGame ID: {game_id}")
                
                # Get details for this specific game
                game_details = matchmaking_failed[matchmaking_failed['game_id'] == game_id]
                
                # for idx, detail in game_details.iterrows():
                #     print(f"  User: {detail['user_id']}")
                #     print(f"  Created by: {detail['created_by']}")
                #     print(f"  Table ID: {detail['table_id']}")
                #     print(f"  Created at: {detail['created_at']}")
                #     print(f"  Updated at: {detail['updated_at']}")
                #     print("  ---")
        
            print(f"\n🔥 VERY SUSPICIOUS CASES (>2 users per failed game_id): {len(suspicious_cases)}")
            print("These are highly anomalous for a 2-player game system!")
            print("-" * 50)
            
            for _, row in suspicious_cases.iterrows():
                game_id = row['game_id']
                user_count = row['user_count']
                print(f"\nGame ID: {game_id} - {user_count} users")
                
                game_details = matchmaking_failed[matchmaking_failed['game_id'] == game_id]
                for idx, detail in game_details.iterrows():
                    print(f"  User: {detail['user_id']} | Created by: {detail['created_by']} | Table: {detail['table_id']}")
        
        # Summary statistics
        print(f"\n📈 SUMMARY:")
        print(f"Normal failure rate: {len(normal_cases)/len(game_user_counts)*100:.1f}%")
        print(f"Anomalous failure rate: {len(anomalous_cases)/len(game_user_counts)*100:.1f}%")
        
        # Check if anomalous cases have different created_by patterns
        if len(anomalous_cases) > 0:
            print(f"\n🔍 ANALYZING FISHY CASES:")
            anomalous_game_ids = anomalous_cases['game_id'].tolist()
            anomalous_details = matchmaking_failed[matchmaking_failed['game_id'].isin(anomalous_game_ids)]
            
            created_by_patterns = anomalous_details.groupby('game_id')['created_by'].apply(list).reset_index()
            
            mixed_patterns = 0
            same_patterns = 0
            
            for _, pattern in created_by_patterns.iterrows():
                creators = pattern['created_by']
                if len(set(creators)) > 1:
                    mixed_patterns += 1
                else:
                    same_patterns += 1
            
            print(f"Games with mixed created_by (rummy-registration + new-game-start): {mixed_patterns}")
            print(f"Games with same created_by: {same_patterns}")
            
            # TIMING ANALYSIS FOR FISHY CASES
            print(f"\n⏱️ TIMING ANALYSIS FOR FISHY CASES:")
            print("-" * 60)
            
            # Convert datetime columns (fix warnings)
            anomalous_details = anomalous_details.copy()  # Fix SettingWithCopyWarning
            date_format = "%B %d, %Y, %I:%M:%S.%f %p"  # Specify exact format to avoid warnings
            anomalous_details.loc[:, 'created_at_dt'] = pd.to_datetime(anomalous_details['created_at'], format=date_format, errors='coerce')
            anomalous_details.loc[:, 'updated_at_dt'] = pd.to_datetime(anomalous_details['updated_at'], format=date_format, errors='coerce')
            
            # Calculate time difference for each user
            anomalous_details.loc[:, 'time_diff_seconds'] = (anomalous_details['updated_at_dt'] - anomalous_details['created_at_dt']).dt.total_seconds()
            
            # Group by game_id and calculate min time for each game
            timing_analysis = []
            
            for game_id in anomalous_game_ids:
                game_data = anomalous_details[anomalous_details['game_id'] == game_id]
                
                if len(game_data) == 2:  # Should be exactly 2 users
                    user1_data = game_data.iloc[0]
                    user2_data = game_data.iloc[1]
                    
                    diff1 = user1_data['time_diff_seconds']
                    diff2 = user2_data['time_diff_seconds']
                    
                    min_time = min(diff1, diff2)
                    max_time = max(diff1, diff2)
                    
                    timing_analysis.append({
                        'game_id': game_id,
                        'user1_created_by': user1_data['created_by'],
                        'user2_created_by': user2_data['created_by'],
                        'user1_time_diff': diff1,
                        'user2_time_diff': diff2,
                        'min_time': min_time,
                        'max_time': max_time,
                        'table_id': user1_data['table_id']
                    })
            
            # Create timing distribution
            timing_df = pd.DataFrame(timing_analysis)
            
            if len(timing_df) > 0:
                # Categorize by min_time
                def categorize_time(min_time):
                    if min_time < 2:
                        return "< 2 seconds"
                    elif min_time < 5:
                        return "2-5 seconds"
                    else:
                        return ">= 5 seconds"
                
                timing_df['time_category'] = timing_df['min_time'].apply(categorize_time)
                
                # Distribution analysis
                time_distribution = timing_df['time_category'].value_counts()
                
                print(f"Distribution of minimum wait times before failure:")
                print("-" * 50)
                
                total_fishy = len(timing_df)
                for category, count in time_distribution.items():
                    percentage = (count / total_fishy) * 100
                    print(f"{category}: {count} games ({percentage:.1f}%)")
                
                print(f"\nDetailed Statistics:")
                print(f"- Total fishy games analyzed: {total_fishy}")
                print(f"- Average min time: {timing_df['min_time'].mean():.2f} seconds")
                print(f"- Median min time: {timing_df['min_time'].median():.2f} seconds")
                print(f"- Fastest failure: {timing_df['min_time'].min():.2f} seconds")
                print(f"- Slowest failure: {timing_df['min_time'].max():.2f} seconds")
                
                # Show some examples from each category
                print(f"\n📋 EXAMPLES FROM EACH CATEGORY:")
                print("-" * 50)
                
                for category in ["< 2 seconds", "2-5 seconds", ">= 5 seconds"]:
                    category_data = timing_df[timing_df['time_category'] == category]
                    if len(category_data) > 0:
                        print(f"\n{category} ({len(category_data)} games):")
                        # Show first 3 examples
                        for i, (_, row) in enumerate(category_data.head(3).iterrows()):
                            print(f"  Game {row['game_id']}: {row['min_time']:.2f}s")
                            print(f"    User1 ({row['user1_created_by']}): {row['user1_time_diff']:.2f}s")
                            print(f"    User2 ({row['user2_created_by']}): {row['user2_time_diff']:.2f}s")
                            print(f"    Table: {row['table_id']}")
                
                # Pattern analysis by timing
                print(f"\n🔍 PATTERN ANALYSIS BY TIMING:")
                print("-" * 50)
                
                # Check if mixed vs same patterns have different timing
                timing_df['pattern_type'] = timing_df.apply(
                    lambda row: 'Mixed' if row['user1_created_by'] != row['user2_created_by'] else 'Same', 
                    axis=1
                )
                
                pattern_timing = timing_df.groupby(['time_category', 'pattern_type']).size().unstack(fill_value=0)
                print("Timing vs Pattern Type:")
                print(pattern_timing)
                
                # Average timing by pattern type
                avg_timing_by_pattern = timing_df.groupby('pattern_type')['min_time'].agg(['mean', 'count'])
                print(f"\nAverage timing by pattern type:")
                for pattern_type, stats in avg_timing_by_pattern.iterrows():
                    print(f"{pattern_type}: {stats['mean']:.2f}s average ({stats['count']} games)")
        
        return {
            'normal_cases': len(normal_cases),
            'anomalous_cases': len(anomalous_cases),
            'suspicious_cases': len(suspicious_cases),
            'total_games': len(game_user_counts)
        }
        
    except Exception as e:
        print(f"Error analyzing game_id patterns: {e}")
        return None


def analyze_top_users_from_slow_games(file_path):
    """
    Analyze top users from games that took ≥5 seconds and show their total matchmaking failure counts
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        print(f"\n{'='*80}")
        print("TOP USERS ANALYSIS FROM SLOW GAMES (≥5 SECONDS)")
        print(f"{'='*80}")
        
        # Group by game_id and count users per game (focus on fishy cases with 2 users)
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        
        # Get fishy cases (2 users per game)
        fishy_cases = game_user_counts[game_user_counts['user_count'] == 2]
        fishy_game_ids = fishy_cases['game_id'].tolist()
        fishy_details = matchmaking_failed[matchmaking_failed['game_id'].isin(fishy_game_ids)]
        
        if len(fishy_details) == 0:
            print("No fishy cases found to analyze.")
            return
        
        # Convert datetime columns for timing analysis (fix warnings)
        fishy_details = fishy_details.copy()  # Fix SettingWithCopyWarning
        date_format = "%B %d, %Y, %I:%M:%S.%f %p"  # Specify exact format to avoid warnings
        fishy_details.loc[:, 'created_at_dt'] = pd.to_datetime(fishy_details['created_at'], format=date_format, errors='coerce')
        fishy_details.loc[:, 'updated_at_dt'] = pd.to_datetime(fishy_details['updated_at'], format=date_format, errors='coerce')
        fishy_details.loc[:, 'time_diff_seconds'] = (fishy_details['updated_at_dt'] - fishy_details['created_at_dt']).dt.total_seconds()
        
        # Find games that took ≥5 seconds
        slow_game_ids = []
        for game_id in fishy_game_ids:
            game_data = fishy_details[fishy_details['game_id'] == game_id]
            if len(game_data) >= 2:
                min_time = game_data['time_diff_seconds'].min()
                if min_time >= 5:  # Games that took ≥5 seconds
                    slow_game_ids.append(game_id)
        
        if len(slow_game_ids) == 0:
            print("No games found that took ≥5 seconds.")
            return
        
        print(f"Found {len(slow_game_ids)} games that took ≥5 seconds")
        
        # Get all users from slow games
        slow_game_users = fishy_details[fishy_details['game_id'].isin(slow_game_ids)]['user_id'].unique()
        
        print(f"Total unique users in slow games: {len(slow_game_users)}")
        
        # Now get total matchmaking failure counts for these users across the entire dataset
        user_total_failures = matchmaking_failed[matchmaking_failed['user_id'].isin(slow_game_users)]['user_id'].value_counts()
        
        # Get top 10 users by total failure count
        top_10_users = user_total_failures.head(10)
        
        print(f"\n📊 TOP 10 USERS BY TOTAL MATCHMAKING FAILURES:")
        print("(Users who appeared in ≥5 second games, ranked by total failure count)")
        print("-" * 70)
        print(f"{'Rank':<4} {'User ID':<26} {'Total Failures':<15} {'% of All Failures'}")
        print("-" * 70)
        
        total_failures_in_dataset = len(matchmaking_failed)
        
        for i, (user_id, failure_count) in enumerate(top_10_users.items(), 1):
            percentage = (failure_count / total_failures_in_dataset) * 100
            print(f"{i:<4} {user_id:<26} {failure_count:<15} {percentage:.2f}%")
        
        # Additional analysis for these top users
        print(f"\n🔍 DETAILED ANALYSIS OF TOP 5 USERS:")
        print("-" * 60)
        
        top_5_users = top_10_users.head(5)
        
        for i, (user_id, total_failures) in enumerate(top_5_users.items(), 1):
            print(f"\n{i}. User: {user_id}")
            print(f"   Total failures: {total_failures}")
            
            # Get all failure records for this user
            user_failures = matchmaking_failed[matchmaking_failed['user_id'] == user_id]
            
            # Analyze created_by pattern
            created_by_pattern = user_failures['created_by'].value_counts()
            print(f"   Created by pattern: {dict(created_by_pattern)}")
            
            # Count how many slow games this user was in
            user_slow_games = len(fishy_details[(fishy_details['user_id'] == user_id) & 
                                              (fishy_details['game_id'].isin(slow_game_ids))])
            print(f"   Slow games (≥5s): {user_slow_games}")
            
            # Show recent failure timestamps (first 3)
            recent_failures = user_failures.head(3)
            print(f"   Recent failures:")
            for _, failure in recent_failures.iterrows():
                print(f"     Game: {failure['game_id']} | Created by: {failure['created_by']} | Time: {failure['created_at']}")
        
        # Summary statistics
        print(f"\n📈 SUMMARY STATISTICS:")
        print("-" * 40)
        print(f"Users analyzed from slow games: {len(slow_game_users)}")
        print(f"Average failures per user: {user_total_failures.mean():.1f}")
        print(f"Median failures per user: {user_total_failures.median():.1f}")
        print(f"Max failures by single user: {user_total_failures.max()}")
        print(f"Min failures by single user: {user_total_failures.min()}")
        
        # Distribution analysis
        failure_distribution = user_total_failures.value_counts().sort_index()
        print(f"\nFailure count distribution:")
        for failure_count, user_count in failure_distribution.head(10).items():
            print(f"  {failure_count} failures: {user_count} users")
        
        return {
            'slow_games_count': len(slow_game_ids),
            'unique_users_in_slow_games': len(slow_game_users),
            'top_10_users': top_10_users.to_dict(),
            'avg_failures_per_user': user_total_failures.mean(),
            'max_failures': user_total_failures.max()
        }
        
    except Exception as e:
        print(f"Error analyzing top users from slow games: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_top_users_by_total_failures(file_path):
    """
    Get top 10 users with highest total matchmaking failure count
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        print(f"\n{'='*80}")
        print("TOP 10 USERS BY TOTAL MATCHMAKING FAILURES")
        print(f"{'='*80}")
        
        # Count total failures per user across entire dataset
        user_failure_counts = matchmaking_failed['user_id'].value_counts()
        
        # Get top 10 users
        top_10_users = user_failure_counts.head(10)
        
        total_failures_in_dataset = len(matchmaking_failed)
        
        print(f"Total matchmaking failures in dataset: {total_failures_in_dataset:,}")
        print(f"Total unique users with failures: {len(user_failure_counts):,}")
        print(f"\n📊 TOP 10 USERS BY FAILURE COUNT:")
        print("-" * 80)
        print(f"{'Rank':<4} {'User ID':<26} {'Total Failures':<15} {'% of All Failures':<15} {'Created By Pattern'}")
        print("-" * 80)
        
        for i, (user_id, failure_count) in enumerate(top_10_users.items(), 1):
            percentage = (failure_count / total_failures_in_dataset) * 100
            
            # Get created_by pattern for this user
            user_failures = matchmaking_failed[matchmaking_failed['user_id'] == user_id]
            created_by_pattern = user_failures['created_by'].value_counts()
            
            # Format the pattern nicely
            pattern_str = ", ".join([f"{cb}: {count}" for cb, count in created_by_pattern.items()])
            
            print(f"{i:<4} {user_id:<26} {failure_count:<15} {percentage:<14.2f}% {pattern_str}")
        
        # Additional statistics
        print(f"\n📈 SUMMARY STATISTICS:")
        print("-" * 50)
        print(f"Average failures per user: {user_failure_counts.mean():.1f}")
        print(f"Median failures per user: {user_failure_counts.median():.1f}")
        print(f"Max failures by single user: {user_failure_counts.max()}")
        print(f"Min failures by single user: {user_failure_counts.min()}")
        
        # Show failure distribution
        failure_distribution = user_failure_counts.value_counts().sort_index()
        print(f"\n📊 FAILURE COUNT DISTRIBUTION:")
        print("-" * 40)
        print("Failures per user | Number of users")
        print("-" * 40)
        for failure_count, user_count in failure_distribution.head(15).items():
            print(f"{failure_count:>15} | {user_count}")
        
        # Show what percentage of failures the top users account for
        top_10_total_failures = top_10_users.sum()
        top_10_percentage = (top_10_total_failures / total_failures_in_dataset) * 100
        
        print(f"\n🎯 TOP 10 USERS IMPACT:")
        print("-" * 40)
        print(f"Top 10 users account for: {top_10_total_failures:,} failures ({top_10_percentage:.1f}% of all failures)")
        print(f"Remaining {len(user_failure_counts)-10:,} users account for: {total_failures_in_dataset-top_10_total_failures:,} failures ({100-top_10_percentage:.1f}%)")
        
        return {
            'top_10_users': top_10_users.to_dict(),
            'total_failures': total_failures_in_dataset,
            'unique_users': len(user_failure_counts),
            'top_10_percentage': top_10_percentage
        }
        
    except Exception as e:
        print(f"Error analyzing top users by total failures: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_critical_failure_users(file_path):
    """
    Analyze users from CRITICAL FAILURES (fishy cases with time >= 5 seconds)
    Shows user_id, count(*) for matchmaking failures where game_id is in critical failure games
    Equivalent to: SELECT user_id, COUNT(*) FROM failures WHERE reason='matchmaking-failed' AND game_id IN (critical_game_ids) GROUP BY user_id ORDER BY COUNT(*) DESC
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        print(f"\n{'='*80}")
        print("CRITICAL FAILURES ANALYSIS - USER-WISE BREAKDOWN")
        print("(Time >= 5 seconds, 2 users per game)")
        print(f"{'='*80}")
        
        # Step 1: Identify CRITICAL FAILURES (fishy cases with 2 users)
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        
        # Get fishy cases (2 users per game) - these are potential critical failures
        fishy_cases = game_user_counts[game_user_counts['user_count'] == 2]
        fishy_game_ids = fishy_cases['game_id'].tolist()
        fishy_details = matchmaking_failed[matchmaking_failed['game_id'].isin(fishy_game_ids)]
        
        if len(fishy_details) == 0:
            print("No fishy cases found to analyze.")
            return
        
        # Step 2: Filter for time >= 5 seconds (fix warnings)
        fishy_details = fishy_details.copy()  # Fix SettingWithCopyWarning
        date_format = "%B %d, %Y, %I:%M:%S.%f %p"  # Specify exact format to avoid warnings
        fishy_details.loc[:, 'created_at_dt'] = pd.to_datetime(fishy_details['created_at'], format=date_format, errors='coerce')
        fishy_details.loc[:, 'updated_at_dt'] = pd.to_datetime(fishy_details['updated_at'], format=date_format, errors='coerce')
        fishy_details.loc[:, 'time_diff_seconds'] = (fishy_details['updated_at_dt'] - fishy_details['created_at_dt']).dt.total_seconds()
        
        # Find games that took ≥5 seconds (CRITICAL FAILURES)
        critical_failure_game_ids = []
        for game_id in fishy_game_ids:
            game_data = fishy_details[fishy_details['game_id'] == game_id]
            if len(game_data) >= 2:
                min_time = game_data['time_diff_seconds'].min()
                if min_time >= 5:  # CRITICAL: Games that took ≥5 seconds
                    critical_failure_game_ids.append(game_id)
        
        if len(critical_failure_game_ids) == 0:
            print("No CRITICAL FAILURES found (time >= 5 seconds).")
            return
        
        print(f"🚨 CRITICAL FAILURES IDENTIFIED:")
        print(f"├── Total fishy games analyzed: {len(fishy_game_ids)}")
        print(f"├── Critical failure games (≥5s): {len(critical_failure_game_ids)}")
        print(f"└── Critical failure rate: {(len(critical_failure_game_ids)/len(fishy_game_ids)*100):.1f}% of fishy cases")
        
        # Step 3: Get all matchmaking failures for these critical game_ids
        # This is equivalent to: SELECT user_id, COUNT(*) FROM matchmaking_failed WHERE game_id IN (critical_failure_game_ids) GROUP BY user_id ORDER BY COUNT(*) DESC
        critical_failure_records = matchmaking_failed[matchmaking_failed['game_id'].isin(critical_failure_game_ids)]
        
        # Group by user_id and count
        user_critical_failure_counts = critical_failure_records['user_id'].value_counts()
        
        print(f"\n📊 CRITICAL FAILURE USER BREAKDOWN:")
        print("(SELECT user_id, COUNT(*) FROM failures WHERE reason='matchmaking-failed' AND game_id IN critical_games GROUP BY user_id ORDER BY COUNT(*) DESC)")
        print("-" * 80)
        print(f"{'Rank':<4} {'User ID':<26} {'Critical Failures':<18} {'% of Critical':<15} {'Game IDs'}")
        print("-" * 80)
        
        total_critical_records = len(critical_failure_records)
        
        for i, (user_id, failure_count) in enumerate(user_critical_failure_counts.items(), 1):
            percentage = (failure_count / total_critical_records) * 100
            
            # Get the specific game_ids this user failed in
            user_game_ids = critical_failure_records[critical_failure_records['user_id'] == user_id]['game_id'].unique()
            game_ids_str = f"{len(user_game_ids)} games"
            if len(user_game_ids) <= 3:
                game_ids_str = ", ".join(user_game_ids[:3])
            else:
                game_ids_str = f"{', '.join(user_game_ids[:2])}... (+{len(user_game_ids)-2} more)"
            
            print(f"{i:<4} {user_id:<26} {failure_count:<18} {percentage:<14.1f}% {game_ids_str}")
            
            # Stop after top 20 to keep output manageable
            if i >= 20:
                break
        
        # Step 4: Show the actual critical failure game_ids array
        print(f"\n🎯 CRITICAL FAILURE GAME IDS ARRAY:")
        print("(Games with 2 users present but failed after ≥5 seconds)")
        print("-" * 60)
        print("Game IDs:")
        
        # Show game IDs in a readable format
        for i, game_id in enumerate(critical_failure_game_ids, 1):
            print(f"{i:>3}. {game_id}")
            if i >= 10:  # Show first 10, then summarize
                remaining = len(critical_failure_game_ids) - 10
                if remaining > 0:
                    print(f"     ... and {remaining} more game IDs")
                break
        
        # Step 5: Additional analysis
        print(f"\n📈 SUMMARY STATISTICS:")
        print("-" * 50)
        print(f"Total critical failure game IDs: {len(critical_failure_game_ids)}")
        print(f"Total critical failure records: {total_critical_records}")
        print(f"Unique users in critical failures: {len(user_critical_failure_counts)}")
        print(f"Average critical failures per user: {user_critical_failure_counts.mean():.1f}")
        print(f"Max critical failures by single user: {user_critical_failure_counts.max()}")
        
        # Show created_by pattern for critical failures
        created_by_pattern = critical_failure_records['created_by'].value_counts()
        print(f"\nCreated by pattern in critical failures:")
        for created_by, count in created_by_pattern.items():
            percentage = (count / total_critical_records) * 100
            print(f"  {created_by}: {count} ({percentage:.1f}%)")
        
        # Return the data for further analysis
        return {
            'critical_game_ids': critical_failure_game_ids,
            'user_failure_counts': user_critical_failure_counts.to_dict(),
            'total_critical_records': total_critical_records,
            'unique_users': len(user_critical_failure_counts),
            'created_by_pattern': created_by_pattern.to_dict()
        }
        
    except Exception as e:
        print(f"Error analyzing critical failure users: {e}")
        import traceback
        traceback.print_exc()
        return None


def print_comprehensive_summary():
    """
    Print a comprehensive summary of all matchmaking analysis findings
    """
    print(f"\n{'='*80}")
    print("🎯 COMPREHENSIVE MATCHMAKING FAILURE ANALYSIS - FULL PICTURE")
    print(f"{'='*80}")
    
    print(f"\n📊 OVERALL SYSTEM HEALTH:")
    print("-" * 50)
    print("• Total Records: 584 matchmaking failures")
    print("• Failure Rate: 100% (all records are failures)")
    print("• Unique Games: 390 game instances")
    print("• Affected Users: 584 users")
    print("• Time Period: Based on CSV data timestamps")
    
    print(f"\n🔍 FAILURE CATEGORIZATION:")
    print("-" * 50)
    print("1. NORMAL CASES (196 games, 50.3%)")
    print("   • 1 user per game_id - Expected behavior")
    print("   • Player joined, waited 30s timeout, no second player")
    print("   • new-game-start: 128 cases (65.3%) - CGP users")
    print("   • rummy-registration: 68 cases (34.7%) - Fresh users")
    print("")
    print("2. FISHY CASES (194 games, 49.7%)")
    print("   • 2 users per game_id - Suspicious behavior")
    print("   • Both players present but game still failed")
    print("   • Mixed patterns: 133 games (68.6%)")
    print("   • Same patterns: 61 games (31.4%)")
    
    print(f"\n⏱️ TIMING ANALYSIS OF FISHY CASES:")
    print("-" * 50)
    print("• Instant Failures (<2s): 141 games (72.7%)")
    print("  - Average: 0.00 seconds")
    print("  - Indicates: Race conditions, validation failures")
    print("")
    print("• Timeout Failures (≥5s): 53 games (27.3%)")
    print("  - Average: 60.00 seconds (full timeout)")
    print("  - Indicates: Stuck matchmaking, deadlock scenarios")
    print("")
    print("• No failures in 2-5 second range (0%)")
    print("  - Confirms bimodal failure pattern")
    
    print(f"\n🎮 USER JOURNEY ANALYSIS:")
    print("-" * 50)
    print("CONTINUING GAMEPLAY (CGP) USERS:")
    print("• Source: new-game-start (261 total failures)")
    print("• Normal cases: 128 (49.0% of CGP failures)")
    print("• Fishy cases: 133 (51.0% of CGP failures)")
    print("• Challenge: Higher fishy case rate suggests CGP issues")
    print("")
    print("FRESH REGISTRATION USERS:")
    print("• Source: rummy-registration (323 total failures)")
    print("• Normal cases: 68 (21.1% of registration failures)")
    print("• Fishy cases: 255 (78.9% of registration failures)")
    print("• Challenge: High fishy rate during onboarding")
    
    print(f"\n🚨 CRITICAL SYSTEM ISSUES IDENTIFIED:")
    print("-" * 50)
    print("1. RACE CONDITIONS (72.7% of fishy cases)")
    print("   • Instant failures when 2 players present")
    print("   • Likely causes: Concurrent table allocation")
    print("   • Impact: Poor user experience, lost revenue")
    print("")
    print("2. MATCHMAKING DEADLOCKS (27.3% of fishy cases)")
    print("   • Full 60s timeout despite 2 players present")
    print("   • System not detecting player readiness")
    print("   • Players wait full timeout unnecessarily")
    print("")
    print("3. ONBOARDING FRICTION")
    print("   • 78.9% of registration failures are fishy")
    print("   • New users experiencing system issues")
    print("   • High abandonment risk during first game")
    
    print(f"\n💡 BUSINESS IMPACT & RECOMMENDATIONS:")
    print("-" * 50)
    print("IMMEDIATE ACTIONS (High Priority):")
    print("• Fix race conditions in table allocation logic")
    print("• Investigate deadlock scenarios in matchmaking")
    print("• Add proper concurrency controls")
    print("• Implement retry mechanisms for instant failures")
    print("")
    print("MEDIUM-TERM IMPROVEMENTS:")
    print("• Optimize CGP user experience (51% fishy rate)")
    print("• Enhance new user onboarding flow")
    print("• Add monitoring for bimodal failure patterns")
    print("• Implement graceful degradation for peak loads")
    print("")
    print("MONITORING & METRICS:")
    print("• Track instant vs timeout failure ratios")
    print("• Monitor mixed vs same pattern distributions")
    print("• Alert on fishy case percentage > 30%")
    print("• Measure user retention post-failure")
    
    print(f"\n📈 SUCCESS METRICS TO TRACK:")
    print("-" * 50)
    print("• Reduce fishy cases from 49.7% to <20%")
    print("• Eliminate instant failures (0s timeouts)")
    print("• Improve CGP success rate")
    print("• Reduce new user drop-off during first game")
    print("• Achieve <5% overall matchmaking failure rate")
    
    print(f"\n🎯 CONCLUSION:")
    print("-" * 50)
    print("Your matchmaking system has a 50/50 split between normal")
    print("and problematic failures. The fishy cases show clear")
    print("technical issues (race conditions & deadlocks) that are")
    print("significantly impacting user experience, especially for")
    print("new users and continuing players. Fixing these issues")
    print("could potentially halve your matchmaking failure rate.")
    
    print(f"\n{'='*80}")


def print_executive_hierarchical_report(file_path):
    """
    Print a hierarchical executive summary report for management based on actual data
    
    Args:
        file_path (str): Path to the CSV file
    """
    if not PANDAS_AVAILABLE:
        print("pandas is not available. Please activate your virtual environment.")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Filter for matchmaking failures only
        matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
        
        # Calculate basic metrics
        total_records = len(df)
        total_failures = len(matchmaking_failed)
        failure_rate = (total_failures / total_records * 100) if total_records > 0 else 0
        
        # Game analysis
        game_user_counts = matchmaking_failed.groupby('game_id')['user_id'].count().reset_index()
        game_user_counts.columns = ['game_id', 'user_count']
        
        normal_cases = game_user_counts[game_user_counts['user_count'] == 1]
        fishy_cases = game_user_counts[game_user_counts['user_count'] == 2]
        suspicious_cases = game_user_counts[game_user_counts['user_count'] > 2]
        
        total_unique_games = len(game_user_counts)
        normal_count = len(normal_cases)
        fishy_count = len(fishy_cases)
        suspicious_count = len(suspicious_cases)
        
        normal_percentage = (normal_count / total_unique_games * 100) if total_unique_games > 0 else 0
        fishy_percentage = (fishy_count / total_unique_games * 100) if total_unique_games > 0 else 0
        
        # Created_by analysis
        created_by_counts = matchmaking_failed['created_by'].value_counts()
        
        # Normal cases breakdown by created_by
        normal_game_ids = normal_cases['game_id'].tolist()
        normal_details = matchmaking_failed[matchmaking_failed['game_id'].isin(normal_game_ids)]
        normal_created_by_counts = normal_details['created_by'].value_counts()
        
        # Fishy cases analysis
        fishy_game_ids = fishy_cases['game_id'].tolist()
        fishy_details = matchmaking_failed[matchmaking_failed['game_id'].isin(fishy_game_ids)]
        fishy_created_by_counts = fishy_details['created_by'].value_counts()
        
        # Pattern analysis for fishy cases
        fishy_details_grouped = fishy_details.groupby('game_id')['created_by'].apply(list).reset_index()
        mixed_patterns = 0
        same_patterns = 0
        
        for _, pattern in fishy_details_grouped.iterrows():
            creators = pattern['created_by']
            if len(set(creators)) > 1:
                mixed_patterns += 1
            else:
                same_patterns += 1
        
        mixed_percentage = (mixed_patterns / fishy_count * 100) if fishy_count > 0 else 0
        same_percentage = (same_patterns / fishy_count * 100) if fishy_count > 0 else 0
        
        # Timing analysis for fishy cases
        timing_stats = {}
        if len(fishy_details) > 0:
            fishy_details = fishy_details.copy()  # Fix SettingWithCopyWarning
            date_format = "%B %d, %Y, %I:%M:%S.%f %p"  # Specify exact format to avoid warnings
            fishy_details.loc[:, 'created_at_dt'] = pd.to_datetime(fishy_details['created_at'], format=date_format, errors='coerce')
            fishy_details.loc[:, 'updated_at_dt'] = pd.to_datetime(fishy_details['updated_at'], format=date_format, errors='coerce')
            fishy_details.loc[:, 'time_diff_seconds'] = (fishy_details['updated_at_dt'] - fishy_details['created_at_dt']).dt.total_seconds()
            
            # Calculate timing categories
            timing_analysis = []
            for game_id in fishy_game_ids:
                game_data = fishy_details[fishy_details['game_id'] == game_id]
                if len(game_data) >= 2:
                    min_time = game_data['time_diff_seconds'].min()
                    max_time = game_data['time_diff_seconds'].max()
                    avg_time = game_data['time_diff_seconds'].mean()
                    
                    timing_analysis.append({
                        'game_id': game_id,
                        'min_time': min_time,
                        'max_time': max_time,
                        'avg_time': avg_time
                    })
            
            if timing_analysis:
                timing_df = pd.DataFrame(timing_analysis)
                
                # Categorize timing
                instant_failures = len(timing_df[timing_df['min_time'] < 2])
                quick_failures = len(timing_df[(timing_df['min_time'] >= 2) & (timing_df['min_time'] < 5)])
                slow_failures = len(timing_df[timing_df['min_time'] >= 5])
                
                timing_stats = {
                    'instant': {'count': instant_failures, 'percentage': (instant_failures / len(timing_df) * 100) if len(timing_df) > 0 else 0},
                    'quick': {'count': quick_failures, 'percentage': (quick_failures / len(timing_df) * 100) if len(timing_df) > 0 else 0},
                    'slow': {'count': slow_failures, 'percentage': (slow_failures / len(timing_df) * 100) if len(timing_df) > 0 else 0},
                    'avg_time': timing_df['avg_time'].mean(),
                    'total_analyzed': len(timing_df)
                }
        
        # User journey analysis
        cgp_failures = created_by_counts.get('new-game-start', 0)
        registration_failures = created_by_counts.get('rummy-registration', 0)
        
        cgp_normal = normal_created_by_counts.get('new-game-start', 0)
        cgp_fishy = fishy_created_by_counts.get('new-game-start', 0)
        registration_normal = normal_created_by_counts.get('rummy-registration', 0)
        registration_fishy = fishy_created_by_counts.get('rummy-registration', 0)
        
        cgp_fishy_rate = (cgp_fishy / cgp_failures * 100) if cgp_failures > 0 else 0
        registration_fishy_rate = (registration_fishy / registration_failures * 100) if registration_failures > 0 else 0
        
        print(f"\n{'='*80}")
        print("📊 MATCHMAKING SYSTEM ANALYSIS - EXECUTIVE SUMMARY")
        print(f"{'='*80}")
        
        print(f"\n🎯 EXECUTIVE OVERVIEW")
        status = "CRITICAL ISSUES IDENTIFIED" if fishy_percentage > 30 else "MODERATE ISSUES" if fishy_percentage > 15 else "SYSTEM HEALTHY"
        print(f"├── System Status: {status}")
        print(f"├── Total Failures Analyzed: {total_failures:,} matchmaking attempts")
        print(f"├── Failure Rate: {failure_rate:.1f}% of all sampled attempts")
        print(f"├── Unique Games Affected: {total_unique_games:,} game instances")
        impact = "HIGH" if fishy_percentage > 30 else "MEDIUM" if fishy_percentage > 15 else "LOW"
        
        print(f"\n📈 FAILURE CATEGORIZATION")
        print(f"├── 🟢 NORMAL FAILURES ({normal_percentage:.1f}% - {normal_count:,} games)")
        print("│   ├── Definition: Single player joined, no second player found")
        print("│   ├── Expected Behavior: 30-second timeout")
        print(f"│   ├── Continuing Players (CGP): {cgp_normal:,} cases ({(cgp_normal/normal_count*100):.1f}%)" if normal_count > 0 else "│   ├── Continuing Players (CGP): 0 cases")
        print(f"│   ├── New Users: {registration_normal:,} cases ({(registration_normal/normal_count*100):.1f}%)" if normal_count > 0 else "│   ├── New Users: 0 cases")
        print("│")
        print(f"└── 🔴 CRITICAL FAILURES ({fishy_percentage:.1f}% - {fishy_count:,} games)")
        print("    ├── Definition: Two players present but game still failed")
        if timing_stats:
            print("    └── Timing Pattern Analysis:")
            print(f"        ├── <2s: {timing_stats['instant']['count']:,} games ({timing_stats['instant']['percentage']:.1f}%)")
            print(f"        ├── 2-5s: {timing_stats['quick']['count']:,} games ({timing_stats['quick']['percentage']:.1f}%)")
            print(f"        └── ≥5s: {timing_stats['slow']['count']:,} games ({timing_stats['slow']['percentage']:.1f}%)")
        
        
        
        return {
            'total_failures': total_failures,
            'fishy_percentage': fishy_percentage,
            'normal_percentage': normal_percentage,
            'mixed_patterns': mixed_patterns,
            'same_patterns': same_patterns,
            'timing_stats': timing_stats,
            'cgp_fishy_rate': cgp_fishy_rate,
            'registration_fishy_rate': registration_fishy_rate
        }
        
    except Exception as e:
        print(f"Error generating executive report: {e}")
        return None


def main():
    """
    Main function to demonstrate CSV reading functionality
    """
    print("CSV Reader - Python CSV Data Reading Tool")
    print("=" * 50)
    
    file_path = "query_result_2025-07-30T11_45_43.43219Z.csv"
    
    if PANDAS_AVAILABLE:
        # Read the CSV file
        df = read_csv_with_pandas(file_path)
        
        if df is not None:
            print(f"\n{'='*60}")
            print("MATCHMAKING FAILURE ANALYSIS")
            print(f"{'='*60}")
            
            # Filter for matchmaking-failed records
            matchmaking_failed = df[df['reason'] == 'matchmaking-failed']
            
            print(f"Total records: {len(df)}")
            print(f"Matchmaking failed records: {len(matchmaking_failed)}")
            print(f"Percentage of failures: {(len(matchmaking_failed)/len(df)*100):.1f}%")
            print("-" * 50)
            
            if len(matchmaking_failed) > 0:
                # Group by created_by and count
                failure_counts = matchmaking_failed.groupby('created_by').size().sort_values(ascending=False)
                
                print("Matchmaking failures grouped by created_by:")
                print("-" * 50)
                
                total_failures = len(matchmaking_failed)
                for created_by, count in failure_counts.items():
                    percentage = (count / total_failures) * 100
                    print(f"{created_by}: {count} failures ({percentage:.1f}%)")
                
                print(f"\nSummary:")
                print(f"- Total unique creators with failures: {len(failure_counts)}")
                print(f"- Most failures by: {failure_counts.index[0]} ({failure_counts.iloc[0]} failures)")
                print(f"- Average failures per creator: {failure_counts.mean():.1f}")
                
                # Show some additional insights
                print(f"\nTop 5 creators with most failures:")
                for i, (created_by, count) in enumerate(failure_counts.head().items()):
                    percentage = (count / total_failures) * 100
                    print(f"{i+1}. {created_by}: {count} failures ({percentage:.1f}%)")
            
            # NEW: Show top 10 users by total failure count
            # analyze_top_users_by_total_failures(file_path)
            
            # Game ID pattern analysis
            analyze_game_id_patterns(file_path)
            
            # Print executive hierarchical report
            print_executive_hierarchical_report(file_path)
            
            # Analyze top users from slow games
            # analyze_top_users_from_slow_games(file_path)
            
            # Analyze critical failure users
            analyze_critical_failure_users(file_path)
                    
    else:
        print("pandas is not available. Please activate your virtual environment.")
        print("Run: source .venv/bin/activate")


if __name__ == "__main__":
    main() 