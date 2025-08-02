#!/usr/bin/env python3
"""
Automated Matchmaking Failure Analyzer with AWS S3 Integration

This script automates the complete process of:
1. Reading CSV file with matchmaking failures and registration IDs
2. Converting GMT timestamps to IST and extracting dates
3. Downloading corresponding log files from AWS S3
4. Extracting and analyzing logs using comprehensive matchmaking diagnosis
5. Generating detailed failure analysis reports

Requirements:
- boto3 (pip install boto3)
- pandas (pip install pandas)
- AWS credentials configured (aws configure or environment variables)

Usage:
    python automated_matchmaking_analyzer.py --csv failures.csv --output-dir ./analysis
"""

import os
import sys
import boto3
import pandas as pd
import zipfile
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import argparse
from typing import List, Dict, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('matchmaking_analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AWSMatchmakingAnalyzer:
    """
    Automated analyzer for matchmaking failures using AWS S3 logs
    """
    
    def __init__(self, bucket_name: str = "prod-rummy-shared-upload-m-bucket", 
                 region: str = "ap-south-1"):
        """
        Initialize the analyzer with AWS S3 configuration
        
        Args:
            bucket_name: S3 bucket name containing the logs
            region: AWS region
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = None
        self.analysis_results = []
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client('s3', region_name=region)
            logger.info(f"✅ Connected to AWS S3 - Region: {region}, Bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to AWS S3: {e}")
            raise
    
    def gmt_to_ist(self, gmt_timestamp: str) -> datetime:
        """
        Convert GMT timestamp to IST (GMT + 5:30)
        
        Args:
            gmt_timestamp: GMT timestamp string
            
        Returns:
            IST datetime object
        """
        try:
            # Parse various timestamp formats
            formats = [
                "%B %d, %Y, %I:%M:%S.%f %p",  # "July 27, 2025, 11:52:11.000 AM"
                "%Y-%m-%d %H:%M:%S.%f",       # "2025-07-27 11:52:11.000"
                "%Y-%m-%dT%H:%M:%S.%fZ",      # ISO format
                "%Y-%m-%d %H:%M:%S"           # Simple format
            ]
            
            gmt_dt = None
            for fmt in formats:
                try:
                    gmt_dt = datetime.strptime(gmt_timestamp.strip(), fmt)
                    break
                except ValueError:
                    continue
            
            if gmt_dt is None:
                raise ValueError(f"Unable to parse timestamp: {gmt_timestamp}")
            
            # Add 5 hours 30 minutes for IST
            ist_dt = gmt_dt + timedelta(hours=5, minutes=30)
            logger.debug(f"Converted {gmt_timestamp} (GMT) → {ist_dt} (IST)")
            return ist_dt
            
        except Exception as e:
            logger.error(f"❌ Error converting timestamp {gmt_timestamp}: {e}")
            raise
    
    def construct_s3_path(self, ist_datetime: datetime) -> str:
        """
        Construct S3 path based on IST datetime
        
        Args:
            ist_datetime: IST datetime object
            
        Returns:
            S3 path string
        """
        path = f"rummy_gameplay_logs/{ist_datetime.year:04d}/{ist_datetime.month:02d}/{ist_datetime.day:02d}/"
        logger.debug(f"Constructed S3 path: {path}")
        return path
    
    def list_s3_objects(self, prefix: str) -> List[str]:
        """
        List all objects in S3 with given prefix
        
        Args:
            prefix: S3 prefix to search
            
        Returns:
            List of S3 object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"⚠️ No objects found with prefix: {prefix}")
                return []
            
            objects = [obj['Key'] for obj in response['Contents']]
            logger.info(f"📂 Found {len(objects)} objects with prefix: {prefix}")
            return objects
            
        except Exception as e:
            logger.error(f"❌ Error listing S3 objects with prefix {prefix}: {e}")
            return []
    
    def find_log_file_for_registration(self, registration_id: str, s3_objects: List[str]) -> Optional[str]:
        """
        Find the specific log file containing the registration ID
        
        Args:
            registration_id: Registration ID to search for
            s3_objects: List of S3 object keys
            
        Returns:
            S3 object key if found, None otherwise
        """
        # Look for zip files that might contain the registration ID
        # Registration IDs are typically in the filename or we need to check content
        potential_files = [obj for obj in s3_objects if obj.endswith('.zip')]
        
        logger.info(f"🔍 Searching for registration ID {registration_id} in {len(potential_files)} zip files")
        
        # For now, return all zip files - we'll check content after download
        return potential_files
    
    def download_s3_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download file from S3 to local path
        
        Args:
            s3_key: S3 object key
            local_path: Local file path to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"⬇️ Downloaded: {s3_key} → {local_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Error downloading {s3_key}: {e}")
            return False
    
    def extract_zip_file(self, zip_path: str, extract_dir: str) -> List[str]:
        """
        Extract zip file and return list of extracted files
        
        Args:
            zip_path: Path to zip file
            extract_dir: Directory to extract to
            
        Returns:
            List of extracted file paths
        """
        try:
            extracted_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                extracted_files = [os.path.join(extract_dir, name) for name in zip_ref.namelist()]
            
            logger.info(f"📦 Extracted {len(extracted_files)} files from {zip_path}")
            return extracted_files
            
        except Exception as e:
            logger.error(f"❌ Error extracting {zip_path}: {e}")
            return []
    
    def search_registration_in_file(self, file_path: str, registration_id: str) -> bool:
        """
        Search for registration ID in a log file
        
        Args:
            file_path: Path to log file
            registration_id: Registration ID to search for
            
        Returns:
            True if found, False otherwise
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                return registration_id in content
        except Exception as e:
            logger.debug(f"Error reading {file_path}: {e}")
            return False
    
    def analyze_log_with_cursor_rule(self, log_file_path: str, registration_id: str) -> Dict:
        """
        Analyze log file using the comprehensive matchmaking failure diagnosis rule
        
        Args:
            log_file_path: Path to log file
            registration_id: Registration ID to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        analysis_result = {
            "registration_id": registration_id,
            "log_file": log_file_path,
            "analysis_timestamp": datetime.now().isoformat(),
            "phases": {
                "phase1_registration": {"status": "UNKNOWN", "details": {}},
                "phase2_table_assignment": {"status": "UNKNOWN", "details": {}},
                "phase3_socket_connection": {"status": "UNKNOWN", "details": {}},
                "phase4_matchmaking_lifecycle": {"status": "UNKNOWN", "details": {}}
            },
            "failure_point": "UNKNOWN",
            "failure_type": "UNKNOWN",
            "recommendations": []
        }
        
        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                log_content = f.read()
            
            logger.info(f"🔬 Analyzing log file: {log_file_path} for registration: {registration_id}")
            
            # Phase 1: Tournament Registration Verification
            phase1_result = self._analyze_phase1_registration(log_content, registration_id)
            analysis_result["phases"]["phase1_registration"] = phase1_result
            
            if phase1_result["status"] == "SUCCESS":
                # Phase 2: Game Table Assignment Verification
                phase2_result = self._analyze_phase2_table_assignment(log_content, registration_id)
                analysis_result["phases"]["phase2_table_assignment"] = phase2_result
                
                if phase2_result["status"] == "SUCCESS":
                    # Phase 3: Gameplay Socket Connection Verification
                    phase3_result = self._analyze_phase3_socket_connection(log_content, registration_id)
                    analysis_result["phases"]["phase3_socket_connection"] = phase3_result
                    
                    if phase3_result["status"] == "SUCCESS":
                        # Phase 4: Matchmaking Lifecycle Analysis
                        phase4_result = self._analyze_phase4_matchmaking_lifecycle(log_content, registration_id)
                        analysis_result["phases"]["phase4_matchmaking_lifecycle"] = phase4_result
                        
                        # Determine final failure point and type
                        analysis_result["failure_point"] = phase4_result.get("failure_point", "UNKNOWN")
                        analysis_result["failure_type"] = phase4_result.get("failure_type", "UNKNOWN")
                    else:
                        analysis_result["failure_point"] = "SOCKET_CONNECTION"
                        analysis_result["failure_type"] = "NETWORK_FAILURE"
                else:
                    analysis_result["failure_point"] = "TABLE_ASSIGNMENT"
                    analysis_result["failure_type"] = "ALLOCATION_FAILURE"
            else:
                analysis_result["failure_point"] = "REGISTRATION"
                analysis_result["failure_type"] = "REGISTRATION_FAILURE"
            
            # Generate recommendations
            analysis_result["recommendations"] = self._generate_recommendations(analysis_result)
            
            logger.info(f"✅ Analysis completed for {registration_id}: Failure at {analysis_result['failure_point']}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing log file {log_file_path}: {e}")
            analysis_result["error"] = str(e)
            return analysis_result
    
    def _analyze_phase1_registration(self, log_content: str, registration_id: str) -> Dict:
        """Analyze Phase 1: Tournament Registration"""
        result = {"status": "UNKNOWN", "details": {}}
        
        # Check 1.1: Registration API Request
        register_request_pattern = r'API New Request: /v1\.0/super/tournament/registerTournament'
        register_requests = re.findall(register_request_pattern, log_content)
        
        if register_requests:
            result["details"]["api_request_found"] = True
            result["details"]["request_count"] = len(register_requests)
            
            # Check 1.2: Registration API Success
            success_pattern = rf'API Success: /v1\.0/super/tournament/registerTournament.*"registrationId":"{registration_id}"'
            success_matches = re.findall(success_pattern, log_content, re.DOTALL)
            
            if success_matches:
                result["status"] = "SUCCESS"
                result["details"]["api_success_found"] = True
                
                # Extract additional details
                success_detail_pattern = rf'"success":true.*"registrationId":"{registration_id}".*"entryFee":([0-9.]+)'
                detail_match = re.search(success_detail_pattern, log_content)
                if detail_match:
                    result["details"]["entry_fee"] = float(detail_match.group(1))
            else:
                result["status"] = "FAILED"
                result["details"]["api_success_found"] = False
                result["details"]["failure_reason"] = "Registration API call failed or registrationId not generated"
        else:
            result["status"] = "FAILED"
            result["details"]["api_request_found"] = False
            result["details"]["failure_reason"] = "No registration API request found"
        
        return result
    
    def _analyze_phase2_table_assignment(self, log_content: str, registration_id: str) -> Dict:
        """Analyze Phase 2: Game Table Assignment"""
        result = {"status": "UNKNOWN", "details": {}}
        
        # Check 2.1: Get Tournament Details API Request
        details_request_pattern = rf'API New Request: /v1\.0/super/tournament/getTournamentDetails.*"registrationId":"{registration_id}"'
        details_requests = re.findall(details_request_pattern, log_content, re.DOTALL)
        
        if details_requests:
            result["details"]["api_request_found"] = True
            
            # Check 2.2: Game Table Assigned Confirmation
            table_assigned_pattern = rf'API Success: /v1\.0/super/tournament/getTournamentDetails.*"registrationId":"{registration_id}".*"registrationStatus":"TABLE_ASSIGNED"'
            assigned_matches = re.findall(table_assigned_pattern, log_content, re.DOTALL)
            
            if assigned_matches:
                result["status"] = "SUCCESS"
                result["details"]["table_assigned"] = True
                
                # Extract game server details
                game_server_pattern = rf'"gameplayServer":\{{[^}}]*"gameId":"([^"]+)"[^}}]*"podip":"([^"]+)"[^}}]*\}}'
                server_match = re.search(game_server_pattern, log_content)
                if server_match:
                    result["details"]["game_id"] = server_match.group(1)
                    result["details"]["pod_ip"] = server_match.group(2)
            else:
                result["status"] = "FAILED"
                result["details"]["table_assigned"] = False
                result["details"]["failure_reason"] = "Table assignment failed or status not TABLE_ASSIGNED"
        else:
            result["status"] = "FAILED"
            result["details"]["api_request_found"] = False
            result["details"]["failure_reason"] = "No getTournamentDetails API request found"
        
        return result
    
    def _analyze_phase3_socket_connection(self, log_content: str, registration_id: str) -> Dict:
        """Analyze Phase 3: Gameplay Socket Connection"""
        result = {"status": "UNKNOWN", "details": {}}
        
        # Check 3.1: Socket Connection Attempt
        socket_url_pattern = rf'Socket url-.*"registrationId":"{registration_id}"'
        socket_attempts = re.findall(socket_url_pattern, log_content)
        
        if socket_attempts:
            result["details"]["connection_attempt_found"] = True
            
            # Check 3.2: Socket Connection Result
            connected_pattern = rf'Socket connected with id-.*"registrationId":"{registration_id}"'
            connected_matches = re.findall(connected_pattern, log_content)
            
            failed_pattern = rf'Socket connection failed-.*"registrationId":"{registration_id}"'
            failed_matches = re.findall(failed_pattern, log_content)
            
            if connected_matches:
                result["status"] = "SUCCESS"
                result["details"]["connection_successful"] = True
                result["details"]["connection_count"] = len(connected_matches)
            elif failed_matches:
                result["status"] = "FAILED"
                result["details"]["connection_successful"] = False
                result["details"]["failure_reason"] = "Socket connection explicitly failed"
            else:
                result["status"] = "FAILED"
                result["details"]["connection_successful"] = False
                result["details"]["failure_reason"] = "No connection success or failure confirmation found"
        else:
            result["status"] = "FAILED"
            result["details"]["connection_attempt_found"] = False
            result["details"]["failure_reason"] = "No socket connection attempt found"
        
        return result
    
    def _analyze_phase4_matchmaking_lifecycle(self, log_content: str, registration_id: str) -> Dict:
        """Analyze Phase 4: Matchmaking Lifecycle"""
        result = {"status": "UNKNOWN", "details": {}, "failure_point": "UNKNOWN", "failure_type": "UNKNOWN"}
        
        # Check 4.1: User Enters Matchmaking Queue
        finding_pattern = rf'eventHandler gameplay socket event-.*"registrationId":"{registration_id}".*"state":"FINDING"'
        finding_matches = re.findall(finding_pattern, log_content, re.DOTALL)
        
        if finding_matches:
            result["details"]["entered_queue"] = True
            
            # Check 4.2: Final Matchmaking Outcome
            # Outcome A: Server-Side Matchmaking Failure
            match_failed_pattern = rf'eventHandler gameplay socket event-.*"registrationId":"{registration_id}".*"en":"MATCH_MAKING_FAILED"'
            failed_matches = re.findall(match_failed_pattern, log_content, re.DOTALL)
            
            # Outcome B: Client-Side Timeout
            timeout_pattern = r'backToLobbyInterval Timer expired'
            timeout_matches = re.findall(timeout_pattern, log_content)
            
            # Outcome C: Successful Match
            round_starting_pattern = rf'eventHandler gameplay socket event-.*"registrationId":"{registration_id}".*"en":"ROUND_STARTING"'
            success_matches = re.findall(round_starting_pattern, log_content, re.DOTALL)
            
            if success_matches:
                result["status"] = "SUCCESS"
                result["details"]["outcome"] = "SUCCESSFUL_MATCH"
                result["details"]["round_started"] = True
                result["failure_point"] = "NO_FAILURE"
                result["failure_type"] = "SUCCESS"
            elif failed_matches:
                result["status"] = "FAILED"
                result["details"]["outcome"] = "SERVER_SIDE_FAILURE"
                result["details"]["match_making_failed"] = True
                result["failure_point"] = "MATCHMAKING_LOGIC"
                result["failure_type"] = "SERVER_SIDE_MATCHMAKING_FAILURE"
            elif timeout_matches:
                result["status"] = "FAILED"
                result["details"]["outcome"] = "CLIENT_SIDE_TIMEOUT"
                result["details"]["client_timeout"] = True
                result["failure_point"] = "SERVER_UNRESPONSIVE"
                result["failure_type"] = "CLIENT_SIDE_TIMEOUT"
            else:
                result["status"] = "FAILED"
                result["details"]["outcome"] = "UNKNOWN_FAILURE"
                result["failure_point"] = "MATCHMAKING_UNKNOWN"
                result["failure_type"] = "UNKNOWN_MATCHMAKING_FAILURE"
        else:
            result["status"] = "FAILED"
            result["details"]["entered_queue"] = False
            result["details"]["failure_reason"] = "User never entered FINDING state"
            result["failure_point"] = "QUEUE_ENTRY"
            result["failure_type"] = "QUEUE_ENTRY_FAILURE"
        
        return result
    
    def _generate_recommendations(self, analysis_result: Dict) -> List[str]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        failure_point = analysis_result.get("failure_point", "UNKNOWN")
        failure_type = analysis_result.get("failure_type", "UNKNOWN")
        
        if failure_point == "REGISTRATION":
            recommendations.extend([
                "Check user wallet balance and session validity",
                "Verify tournament availability and user eligibility",
                "Review registration API error responses"
            ])
        elif failure_point == "TABLE_ASSIGNMENT":
            recommendations.extend([
                "Investigate game server allocation service health",
                "Check available game server pool capacity",
                "Review backend infrastructure scaling"
            ])
        elif failure_point == "SOCKET_CONNECTION":
            recommendations.extend([
                "Test network connectivity to game servers",
                "Check specific game server instance health",
                "Verify firewall and load balancer configurations"
            ])
        elif failure_point == "MATCHMAKING_LOGIC":
            recommendations.extend([
                "Investigate game server matchmaking algorithm",
                "Check for race conditions or deadlocks",
                "Review player liquidity for the time period",
                f"Examine specific pod IP: {analysis_result['phases']['phase2_table_assignment']['details'].get('pod_ip', 'unknown')}"
            ])
        elif failure_point == "SERVER_UNRESPONSIVE":
            recommendations.extend([
                "Check game server responsiveness and health",
                "Investigate potential server crashes or hangs",
                "Review server resource utilization"
            ])
        
        return recommendations
    
    def process_csv_file(self, csv_file_path: str, output_dir: str) -> None:
        """
        Process CSV file with matchmaking failures
        
        Args:
            csv_file_path: Path to CSV file
            output_dir: Output directory for analysis results
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            logger.info(f"📊 Loaded CSV file: {csv_file_path} with {len(df)} records")
            
            # Validate required columns
            required_columns = ['registrationId', 'created_at']  # Adjust as needed
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Process each record
            total_records = len(df)
            successful_analyses = 0
            
            for index, row in df.iterrows():
                registration_id = str(row['registrationId']).strip()
                created_at = str(row['created_at']).strip()
                
                logger.info(f"🔄 Processing {index + 1}/{total_records}: {registration_id}")
                
                try:
                    # Convert GMT to IST
                    ist_datetime = self.gmt_to_ist(created_at)
                    
                    # Construct S3 path
                    s3_prefix = self.construct_s3_path(ist_datetime)
                    
                    # List S3 objects
                    s3_objects = self.list_s3_objects(s3_prefix)
                    
                    if not s3_objects:
                        logger.warning(f"⚠️ No log files found for {registration_id} on {ist_datetime.date()}")
                        continue
                    
                    # Find potential log files
                    potential_files = self.find_log_file_for_registration(registration_id, s3_objects)
                    
                    if not potential_files:
                        logger.warning(f"⚠️ No potential log files found for {registration_id}")
                        continue
                    
                    # Download and analyze each potential file
                    log_found = False
                    for s3_key in potential_files:
                        # Create local file path
                        local_filename = os.path.basename(s3_key)
                        local_zip_path = os.path.join(output_dir, f"{registration_id}_{local_filename}")
                        
                        # Download file
                        if self.download_s3_file(s3_key, local_zip_path):
                            # Extract zip file
                            extract_dir = os.path.join(output_dir, f"{registration_id}_extracted")
                            extracted_files = self.extract_zip_file(local_zip_path, extract_dir)
                            
                            # Search for registration ID in extracted files
                            for extracted_file in extracted_files:
                                if self.search_registration_in_file(extracted_file, registration_id):
                                    logger.info(f"🎯 Found registration {registration_id} in {extracted_file}")
                                    
                                    # Analyze the log file
                                    analysis_result = self.analyze_log_with_cursor_rule(extracted_file, registration_id)
                                    self.analysis_results.append(analysis_result)
                                    
                                    # Save individual analysis result
                                    result_file = os.path.join(output_dir, f"{registration_id}_analysis.json")
                                    with open(result_file, 'w') as f:
                                        json.dump(analysis_result, f, indent=2)
                                    
                                    log_found = True
                                    successful_analyses += 1
                                    break
                            
                            if log_found:
                                break
                            
                            # Clean up zip file
                            os.remove(local_zip_path)
                    
                    if not log_found:
                        logger.warning(f"⚠️ Registration ID {registration_id} not found in any log files")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {registration_id}: {e}")
                    continue
            
            # Generate summary report
            self._generate_summary_report(output_dir, total_records, successful_analyses)
            logger.info(f"🎉 Processing completed: {successful_analyses}/{total_records} successful analyses")
            
        except Exception as e:
            logger.error(f"❌ Error processing CSV file: {e}")
            raise
    
    def _generate_summary_report(self, output_dir: str, total_records: int, successful_analyses: int) -> None:
        """Generate summary report of all analyses"""
        try:
            summary = {
                "analysis_summary": {
                    "total_records": total_records,
                    "successful_analyses": successful_analyses,
                    "success_rate": f"{(successful_analyses/total_records*100):.1f}%" if total_records > 0 else "0%",
                    "analysis_timestamp": datetime.now().isoformat()
                },
                "failure_point_distribution": {},
                "failure_type_distribution": {},
                "recommendations": {}
            }
            
            # Analyze failure patterns
            failure_points = [result.get("failure_point", "UNKNOWN") for result in self.analysis_results]
            failure_types = [result.get("failure_type", "UNKNOWN") for result in self.analysis_results]
            
            # Count distributions
            from collections import Counter
            summary["failure_point_distribution"] = dict(Counter(failure_points))
            summary["failure_type_distribution"] = dict(Counter(failure_types))
            
            # Generate aggregated recommendations
            all_recommendations = []
            for result in self.analysis_results:
                all_recommendations.extend(result.get("recommendations", []))
            
            summary["recommendations"] = dict(Counter(all_recommendations))
            
            # Save summary report
            summary_file = os.path.join(output_dir, "analysis_summary.json")
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            # Generate readable report
            readable_report = self._generate_readable_report(summary)
            report_file = os.path.join(output_dir, "analysis_report.txt")
            with open(report_file, 'w') as f:
                f.write(readable_report)
            
            logger.info(f"📋 Summary report generated: {summary_file}")
            logger.info(f"📄 Readable report generated: {report_file}")
            
        except Exception as e:
            logger.error(f"❌ Error generating summary report: {e}")
    
    def _generate_readable_report(self, summary: Dict) -> str:
        """Generate human-readable analysis report"""
        report = []
        report.append("=" * 80)
        report.append("🔍 AUTOMATED MATCHMAKING FAILURE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary section
        analysis_summary = summary["analysis_summary"]
        report.append("📊 ANALYSIS SUMMARY")
        report.append("-" * 40)
        report.append(f"Total Records Processed: {analysis_summary['total_records']}")
        report.append(f"Successful Analyses: {analysis_summary['successful_analyses']}")
        report.append(f"Success Rate: {analysis_summary['success_rate']}")
        report.append(f"Analysis Timestamp: {analysis_summary['analysis_timestamp']}")
        report.append("")
        
        # Failure point distribution
        report.append("🎯 FAILURE POINT DISTRIBUTION")
        report.append("-" * 40)
        for point, count in summary["failure_point_distribution"].items():
            percentage = (count / analysis_summary['successful_analyses'] * 100) if analysis_summary['successful_analyses'] > 0 else 0
            report.append(f"├── {point}: {count} ({percentage:.1f}%)")
        report.append("")
        
        # Failure type distribution
        report.append("🔧 FAILURE TYPE DISTRIBUTION")
        report.append("-" * 40)
        for ftype, count in summary["failure_type_distribution"].items():
            percentage = (count / analysis_summary['successful_analyses'] * 100) if analysis_summary['successful_analyses'] > 0 else 0
            report.append(f"├── {ftype}: {count} ({percentage:.1f}%)")
        report.append("")
        
        # Top recommendations
        report.append("💡 TOP RECOMMENDATIONS")
        report.append("-" * 40)
        sorted_recommendations = sorted(summary["recommendations"].items(), key=lambda x: x[1], reverse=True)
        for rec, count in sorted_recommendations[:10]:  # Top 10
            report.append(f"├── {rec} (mentioned {count} times)")
        report.append("")
        
        report.append("=" * 80)
        report.append("For detailed analysis of individual cases, check the *_analysis.json files")
        report.append("=" * 80)
        
        return "\n".join(report)

def main():
    """Main function to run the automated analyzer"""
    parser = argparse.ArgumentParser(description="Automated Matchmaking Failure Analyzer with AWS S3 Integration")
    parser.add_argument("--csv", required=True, help="Path to CSV file containing matchmaking failures")
    parser.add_argument("--output-dir", default="./analysis_output", help="Output directory for analysis results")
    parser.add_argument("--bucket", default="prod-rummy-shared-upload-m-bucket", help="S3 bucket name")
    parser.add_argument("--region", default="ap-south-1", help="AWS region")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize analyzer
        analyzer = AWSMatchmakingAnalyzer(bucket_name=args.bucket, region=args.region)
        
        # Process CSV file
        analyzer.process_csv_file(args.csv, args.output_dir)
        
        logger.info("🎉 Analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 