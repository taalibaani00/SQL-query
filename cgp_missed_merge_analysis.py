#!/usr/bin/env python3
"""
CGP Missed Merge Analysis

Analyzes gameplay data to quantify how many matchmaking failures could have been
prevented by merging waiting players from Continuous Gameplay (CGP) with
other waiting players (either CGP or new registrations).
"""

import os
import sys
import pandas as pd
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

def plot_missed_opportunities(missed_opportunities, output_filename="missed_merges_timeline.png"):
    """
    Generates and saves a scatter plot showing all matchmaking requests as points,
    with CGP requests and new registration requests clearly differentiated.
    """
    if not MATPLOTLIB_AVAILABLE:
        print("\n⚠️  Matplotlib not found. Skipping graph generation.")
        print("   To install it, run: pip install matplotlib")
        return

    if not missed_opportunities:
        return  # Nothing to plot

    print(f"\n📈 Generating scatter plot of all matchmaking requests...")

    # Collect all individual player requests from missed opportunities
    cgp_requests = []
    new_requests = []
    
    for opp in missed_opportunities:
        timestamp = opp['timestamp']
        for player in opp['players_involved']:
            if player['created_by'] == 'new-game-start':
                cgp_requests.append({
                    'timestamp': timestamp,
                    'user_id': player['user_id'],
                    'table_id': player['table_id']
                })
            elif player['created_by'] == 'rummy-registerations':
                new_requests.append({
                    'timestamp': timestamp,
                    'user_id': player['user_id'],
                    'table_id': player['table_id']
                })

    if not cgp_requests and not new_requests:
        print("No requests to plot.")
        return

    # Create the scatter plot
    fig, ax = plt.subplots(figsize=(20, 12))

    # Plot CGP requests (larger, red circles)
    if cgp_requests:
        cgp_times = [req['timestamp'] for req in cgp_requests]
        cgp_y = [1] * len(cgp_requests)  # All at y=1 level
        ax.scatter(cgp_times, cgp_y, 
                  s=200, c='red', marker='o', alpha=0.8, 
                  label=f'CGP Players Waiting ({len(cgp_requests)})', 
                  edgecolors='darkred', linewidth=2, zorder=3)

    # Plot New registration requests (smaller, blue triangles)
    if new_requests:
        new_times = [req['timestamp'] for req in new_requests]
        new_y = [0.5] * len(new_requests)  # All at y=0.5 level
        ax.scatter(new_times, new_y, 
                  s=100, c='blue', marker='^', alpha=0.7, 
                  label=f'New Registration Players ({len(new_requests)})', 
                  edgecolors='darkblue', linewidth=1, zorder=2)

    # Add vertical lines to group simultaneous failures
    unique_timestamps = sorted(set([opp['timestamp'] for opp in missed_opportunities]))
    for i, timestamp in enumerate(unique_timestamps):
        # Find the opportunity for this timestamp
        opp = next(o for o in missed_opportunities if o['timestamp'] == timestamp)
        
        # Draw a light vertical line to group the simultaneous failures
        ax.axvline(x=timestamp, color='gray', linestyle='--', alpha=0.3, zorder=1)
        
        # Add a text annotation showing the missed opportunity
        ax.annotate(f'Missed Merge\n{opp["player_count"]} players', 
                   xy=(timestamp, 1.3), 
                   xytext=(timestamp, 1.5 + (i % 3) * 0.2),
                   ha='center', va='bottom', fontsize=9,
                   bbox=dict(boxstyle="round,pad=0.3", facecolor='yellow', alpha=0.7),
                   arrowprops=dict(arrowstyle='->', color='orange', alpha=0.7))

    # Formatting the plot
    ax.set_title('Timeline: CGP Players vs New Registration Players\n(Missed Merge Opportunities)', 
                fontsize=18, pad=30)
    ax.set_xlabel('Time of Matchmaking Failure', fontsize=14, labelpad=15)
    ax.set_ylabel('Player Type', fontsize=14, labelpad=15)
    
    # Set y-axis labels
    ax.set_yticks([0.5, 1.0])
    ax.set_yticklabels(['New Registration', 'CGP (Waiting)'])
    ax.set_ylim(0, 2)
    
    # Improve x-axis date formatting
    fig.autofmt_xdate(rotation=45)
    formatter = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
    ax.xaxis.set_major_formatter(formatter)
    
    # Add grid for better readability
    ax.grid(axis='x', linestyle=':', alpha=0.5)
    ax.legend(fontsize=12, loc='upper right')
    
    # Add summary text box
    total_cgp = len(cgp_requests)
    total_new = len(new_requests)
    total_opportunities = len(missed_opportunities)
    
    summary_text = f"""SUMMARY:
• {total_opportunities} missed merge opportunities
• {total_cgp} CGP players affected
• {total_new} new players affected
• Total preventable failures: {total_cgp + total_new}"""
    
    ax.text(0.02, 0.98, summary_text, transform=ax.transAxes, 
           fontsize=11, verticalalignment='top',
           bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.8))

    plt.tight_layout()

    try:
        plt.savefig(output_filename, dpi=300, bbox_inches='tight')
        print(f"✅ Scatter plot saved successfully to: {output_filename}")
        print(f"📊 The graph shows:")
        print(f"   • Red circles = CGP players waiting for matches")
        print(f"   • Blue triangles = New players registering")
        print(f"   • Vertical dashed lines = Simultaneous failures (missed opportunities)")
        print(f"   • Yellow boxes = Annotations showing how many players could have been merged")
    except Exception as e:
        print(f"❌ Error saving graph: {e}")

def find_peak_traffic_window(df, window_hours=2):
    """
    Finds the 2-hour window with the highest matchmaking activity.
    Returns the filtered dataframe for that peak period.
    """
    print(f"\n🔍 Finding peak {window_hours}-hour traffic window...")
    
    # Ensure timestamps are datetime
    df['created_at_dt'] = pd.to_datetime(df['created_at'])
    df['updated_at_dt'] = pd.to_datetime(df['updated_at'])
    
    # Get all matchmaking failures
    failures_df = df[df['reason'] == 'matchmaking-failed'].copy()
    
    if len(failures_df) == 0:
        print("❌ No matchmaking failures found.")
        return None
    
    # Use failure time (updated_at) to find peak periods
    failures_df['hour'] = failures_df['updated_at_dt'].dt.floor('H')
    
    # Count failures per hour
    hourly_counts = failures_df.groupby('hour').size().reset_index(name='failure_count')
    
    if len(hourly_counts) < window_hours:
        print(f"❌ Not enough data for {window_hours}-hour window analysis.")
        return df  # Return full dataset
    
    # Find the best 2-hour consecutive window
    best_start_hour = None
    max_failures = 0
    
    for i in range(len(hourly_counts) - window_hours + 1):
        window_failures = hourly_counts.iloc[i:i+window_hours]['failure_count'].sum()
        if window_failures > max_failures:
            max_failures = window_failures
            best_start_hour = hourly_counts.iloc[i]['hour']
    
    if best_start_hour is None:
        print("❌ Could not find peak traffic window.")
        return df
    
    # Define the peak window
    peak_start = best_start_hour
    peak_end = peak_start + pd.Timedelta(hours=window_hours)
    
    print(f"📊 Peak {window_hours}-hour window identified:")
    print(f"├── Start: {peak_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"├── End: {peak_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"└── Total failures in window: {max_failures}")
    
    # Filter the original dataset to this peak window
    peak_df = df[
        (df['updated_at_dt'] >= peak_start) & 
        (df['updated_at_dt'] < peak_end)
    ].copy()
    
    print(f"📈 Peak window dataset: {len(peak_df):,} total records")
    
    return peak_df

def analyze_missed_merges(df):
    """
    Analyzes the data to find instances where matchmaking failed for CGP players
    but could have been resolved by merging them with other waiting players.
    """
    if df is None or len(df) == 0:
        print("❌ No data available for analysis.")
        return

    print(f"\n{'='*80}")
    print(f"🔄 ANALYSIS: MISSED MERGE OPPORTUNITIES FOR CGP")
    print(f"{'='*80}")
    print("This analysis identifies matchmaking failures that could have been prevented")
    print("by matching waiting 'Continuous Gameplay' players with each other.\n")

    # First, find and focus on peak traffic period
    peak_df = find_peak_traffic_window(df, window_hours=2)
    if peak_df is None:
        return
    
    print(f"\n{'='*60}")
    print(f"🎯 FOCUSING ON PEAK 2-HOUR WINDOW")
    print(f"{'='*60}")

    # 1. Ensure timestamps are in datetime format, handling potential errors
    try:
        peak_df['created_at_dt'] = pd.to_datetime(peak_df['created_at'])
        peak_df['updated_at_dt'] = pd.to_datetime(peak_df['updated_at'])
    except Exception as e:
        print(f"❌ Error converting timestamp columns: {e}")
        print("Please ensure 'created_at' and 'updated_at' columns are in a valid format.")
        return

    # 2. Filter for all matchmaking failures in peak window
    failures_df = peak_df[peak_df['reason'] == 'matchmaking-failed'].copy()
    if len(failures_df) == 0:
        print("✅ No 'matchmaking-failed' events found in the peak window.")
        return

    print(f"📊 Peak window analysis:")
    print(f"├── Total matchmaking failures: {len(failures_df):,}")
    print(f"├── CGP failures: {len(failures_df[failures_df['created_by'] == 'new-game-start']):,}")
    print(f"└── New registration failures: {len(failures_df[failures_df['created_by'] == 'rummy-registerations']):,}")

    # 3. Use the failure time (updated_at) as the key identifier for a timeout event.
    # We round the time to the nearest second to group players who timed out together.
    failures_df['failure_time_rounded'] = failures_df['updated_at_dt'].dt.round('1S')

    # 4. Group by the rounded failure time to find players who failed simultaneously.
    # These groups represent a "missed merge opportunity".
    failed_groups = failures_df.groupby('failure_time_rounded')

    missed_opportunities = []
    for timestamp, group in failed_groups:
        # A missed opportunity requires at least 2 players failing together.
        if len(group) >= 2:
            # It's only a CGP missed opportunity if at least ONE of the players was a CGP player.
            is_cgp_opportunity = (group['created_by'] == 'new-game-start').any()
            
            if is_cgp_opportunity:
                opportunity = {
                    'timestamp': timestamp,
                    'player_count': len(group),
                    'players_involved': group[['user_id', 'created_by', 'table_id']].to_dict('records'),
                    'cgp_players_count': len(group[group['created_by'] == 'new-game-start']),
                    'new_players_count': len(group[group['created_by'] == 'rummy-registerations'])
                }
                missed_opportunities.append(opportunity)

    # 5. Report the findings
    if not missed_opportunities:
        print("✅ No clear missed merge opportunities found in the peak window.")
        print("This means there were no instances where multiple players timed out simultaneously.")
        return

    total_opportunities = len(missed_opportunities)
    total_impacted_players = sum(opp['player_count'] for opp in missed_opportunities)
    total_cgp_players_impacted = sum(opp['cgp_players_count'] for opp in missed_opportunities)

    print(f"\n📊 PEAK WINDOW FINDINGS:")
    print(f"├── Found {total_opportunities} distinct moments where merges could have happened.")
    print(f"├── Total players affected: {total_impacted_players}")
    print(f"├── Of which were CGP players: {total_cgp_players_impacted}")
    print(f"└── Average players per missed merge: {total_impacted_players / total_opportunities:.1f}")

    print(f"\n🔥 DETAILED MISSED OPPORTUNITIES (All in peak window):")
    print("-" * 80)
    
    # Sort by the number of players involved to show the biggest missed opportunities first
    missed_opportunities.sort(key=lambda x: x['player_count'], reverse=True)

    for i, opp in enumerate(missed_opportunities, 1):
        print(f"{i:2d}. Timestamp: {opp['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    ├── Players Failed: {opp['player_count']} ({opp['cgp_players_count']} CGP, {opp['new_players_count']} New)")
        print(f"    └── Players Involved:")
        for player in opp['players_involved']:
            # Using .get() for safety in case a column is missing
            user_id = player.get('user_id', 'N/A')
            created_by = player.get('created_by', 'N/A')
            table_id = player.get('table_id', 'N/A')
            print(f"        ├── User: {user_id} | Type: {created_by} | on Table: {table_id}")
        print()
    
    # 6. Generate the visual plot with peak window data
    output_filename = f"peak_2hour_missed_merges_{peak_df['updated_at_dt'].min().strftime('%Y%m%d_%H%M')}.png"
    plot_missed_opportunities(missed_opportunities, output_filename)

    print(f"\n🎯 RECOMMENDATION:")
    print("├── Implement a centralized 'waiting pool' for continuous gameplay players.")
    print("└── This will allow the system to match these waiting players with each other,")
    print("    drastically reducing these preventable failures and improving player retention.")
    print(f"\n{'='*80}")

def main():
    """
    Main function to run the analysis.
    """
    print("🃏 Rummy CGP Missed Merge Analysis Tool")
    print("=" * 60)

    # Check for a file path from command-line arguments first
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Received file path from command line: {file_path}")
    else:
        # Default file path - update this or pass the path as an argument
        file_path = "/Users/karansunkariya/Downloads/query_result_2025-06-12T16_15_07.651272Z.csv"
        print(f"Using default file path: {file_path}")
        print("You can also provide a path as a command-line argument.")

    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        print("Please update the file_path variable or provide the correct path as an argument.")
        return

    # Run the analysis
    try:
        print(f"📖 Reading full gameplay data from: {file_path}")
        raw_df = pd.read_csv(file_path)
        print(f"Found {len(raw_df):,} total records.")

        analyze_missed_merges(raw_df)

    except Exception as e:
        print(f"❌ Error reading or processing the CSV for analysis: {e}")
        return

if __name__ == "__main__":
    main() 