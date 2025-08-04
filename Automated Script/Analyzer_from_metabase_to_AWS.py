#!/usr/bin/env python3
"""
Analyzer from Metabase to AWS - Complete Matchmaking Failure Analysis Pipeline

This script provides end-to-end analysis of matchmaking failures by:
1. Fetching game data from Metabase MySQL database
2. Extracting game IDs and fetching registration details from MongoDB
3. Downloading corresponding AWS logs for registrations (version >=448)
4. Analyzing matchmaking failures using comprehensive cursor rule
5. Generating detailed reports categorized by version and failure types

Flow:
User Input (Date/Time) → Metabase MySQL → CSV → Game IDs → Metabase MongoDB → 
Registration IDs → AWS S3 Logs → Cursor Rule Analysis → Failure Report

Requirements:
- Metabase API access
- AWS S3 access  
- Environment variables configured in .env
- Cursor rule for matchmaking analysis

Usage:
    python Analyzer_from_metabase_to_AWS.py
"""

import os
import sys
import json
import requests
import pandas as pd
import boto3
import zipfile
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure comprehensive logging
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper())
log_file_path = os.getenv('LOG_FILE_PATH', 'metabase_aws_analyzer.log')

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MetabaseToAWSAnalyzer:
    """
    Complete pipeline analyzer from Metabase databases to AWS logs analysis
    """
    
    def __init__(self):
        """Initialize analyzer with credentials from environment variables"""
        logger.info("🚀 Initializing Metabase to AWS Analyzer")
        
        # Load all configuration from environment
        self.config = self._load_configuration()
        
        # Initialize connections
        self.metabase_session = None
        self.s3_client = None
        self.analysis_results = []
        
        # File paths
        self.csv_files_dir = Path("CSV files")
        self.logs_files_dir = Path("logs files")
        
        # Create directories if they don't exist
        self.csv_files_dir.mkdir(exist_ok=True)
        self.logs_files_dir.mkdir(exist_ok=True)
        
        # Initialize connections
        self._initialize_metabase_connection()
        self._initialize_aws_connection()
        
    def _load_configuration(self) -> Dict:
        """Load all configuration from environment variables"""
        config = {
            # Metabase Configuration
            'metabase_url': os.getenv('METABASE_URL', ''),
            'metabase_username': os.getenv('METABASE_USERNAME', ''),
            'metabase_password': os.getenv('METABASE_PASSWORD', ''),
            'metabase_api_key': os.getenv('METABASE_API_KEY', ''),
            
            # Database Names
            'mysql_database': os.getenv('MYSQL_DATABASE_NAME', 'MysqlRummyGameplay'),
            'mongodb_database': os.getenv('MONGODB_DATABASE_NAME', 'MongoDB Main'),
            'mongodb_collection': os.getenv('MONGODB_COLLECTION_NAME', 'Registrations'),
            
            # AWS Configuration
            'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'aws_region': os.getenv('AWS_DEFAULT_REGION', 'ap-south-1'),
            'aws_s3_bucket': os.getenv('AWS_S3_BUCKET', 'prod-rummy-shared-upload-m-bucket'),
            
            # Analysis Configuration
            'minimum_version': int(os.getenv('MINIMUM_VERSION_ANALYSIS', '448')),
            'ist_offset_hours': int(os.getenv('IST_OFFSET_HOURS', '5')),
            'ist_offset_minutes': int(os.getenv('IST_OFFSET_MINUTES', '30')),
            'max_parallel_requests': int(os.getenv('MAX_PARALLEL_REQUESTS', '5')),
        }
        
        # Log configuration (non-sensitive parts only)
        logger.info("🔧 Configuration loaded:")
        logger.info(f"   Metabase URL: {config['metabase_url']}")
        logger.info(f"   MySQL Database: {config['mysql_database']}")
        logger.info(f"   MongoDB Database: {config['mongodb_database']}")
        logger.info(f"   Minimum Version for Analysis: {config['minimum_version']}")
        logger.info(f"   AWS S3 Bucket: {config['aws_s3_bucket']}")
        
        return config
    
    def _initialize_metabase_connection(self):
        """Initialize connection to Metabase API"""
        logger.info("🔗 Initializing Metabase connection...")
        
        try:
            self.metabase_session = requests.Session()
            
            # Method 1: API Key Authentication (Preferred)
            if self.config['metabase_api_key']:
                self.metabase_session.headers.update({
                    'X-API-KEY': self.config['metabase_api_key'],
                    'Content-Type': 'application/json'
                })
                logger.info("✅ Using Metabase API Key authentication")
                
            # Method 2: Username/Password Authentication
            elif self.config['metabase_username'] and self.config['metabase_password']:
                auth_url = f"{self.config['metabase_url']}/api/session"
                auth_data = {
                    'username': self.config['metabase_username'],
                    'password': self.config['metabase_password']
                }
                
                response = self.metabase_session.post(auth_url, json=auth_data)
                response.raise_for_status()
                
                session_token = response.json()['id']
                self.metabase_session.headers.update({
                    'X-Metabase-Session': session_token,
                    'Content-Type': 'application/json'
                })
                logger.info("✅ Using Metabase username/password authentication")
                
            else:
                raise ValueError("No Metabase authentication method configured")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize Metabase connection: {e}")
            raise
    
    def _initialize_aws_connection(self):
        """Initialize AWS S3 connection"""
        logger.info("☁️ Initializing AWS S3 connection...")
        
        try:
            session_kwargs = {'region_name': self.config['aws_region']}
            
            if self.config['aws_access_key'] and self.config['aws_secret_key']:
                session_kwargs.update({
                    'aws_access_key_id': self.config['aws_access_key'],
                    'aws_secret_access_key': self.config['aws_secret_key']
                })
                session = boto3.Session(**session_kwargs)
                self.s3_client = session.client('s3')
                logger.info("✅ AWS S3 connected using environment credentials")
            else:
                self.s3_client = boto3.client('s3', region_name=self.config['aws_region'])
                logger.info("✅ AWS S3 connected using default credential chain")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize AWS connection: {e}")
            raise
    
    def get_user_input(self) -> Tuple[datetime, datetime]:
        """Get date/time input from user for analysis period"""
        print("\n" + "="*60)
        print("🎯 METABASE TO AWS ANALYZER - INPUT REQUIRED")
        print("="*60)
        
        while True:
            try:
                print("\n📅 Enter the analysis time period:")
                
                # Get start date/time
                start_date_str = input("Start Date (YYYY-MM-DD): ").strip()
                start_time_str = input("Start Time (HH:MM): ").strip() or "00:00"
                
                # Get end date/time
                end_date_str = input("End Date (YYYY-MM-DD, press Enter for same day): ").strip()
                if not end_date_str:
                    end_date_str = start_date_str
                end_time_str = input("End Time (HH:MM): ").strip() or "23:59"
                
                # Parse dates
                start_datetime = datetime.strptime(f"{start_date_str} {start_time_str}", "%Y-%m-%d %H:%M")
                end_datetime = datetime.strptime(f"{end_date_str} {end_time_str}", "%Y-%m-%d %H:%M")
                
                if start_datetime >= end_datetime:
                    print("❌ Start time must be before end time. Please try again.")
                    continue
                
                print(f"\n✅ Analysis Period: {start_datetime} to {end_datetime}")
                confirm = input("Confirm this time period? (y/N): ").strip().lower()
                
                if confirm == 'y':
                    logger.info(f"📊 User selected analysis period: {start_datetime} to {end_datetime}")
                    return start_datetime, end_datetime
                
            except ValueError as e:
                print(f"❌ Invalid date/time format: {e}")
                print("💡 Please use YYYY-MM-DD for date and HH:MM for time")
                continue
    
    def fetch_mysql_data(self, start_time: datetime, end_time: datetime) -> str:
        """
        Fetch game data from Metabase MySQL database using the SQL query
        Returns: CSV file path
        """
        logger.info("🗃️ Fetching data from MySQL database via Metabase...")
        
        try:
            # Read SQL query from file
            sql_file_path = Path("SQL queries/fetch_data_query.sql")
            with open(sql_file_path, 'r') as f:
                sql_query = f.read()
            
            # Clean up SQL query (remove comments, extra whitespace)
            sql_lines = []
            for line in sql_query.split('\n'):
                line = line.strip()
                if line and not line.startswith('//') and not line.startswith('SQL Query:'):
                    sql_lines.append(line)
            
            clean_sql = ' '.join(sql_lines)
            logger.info(f"📝 Using SQL query: {clean_sql[:100]}...")
            
            # Prepare Metabase query request
            # Note: This might need adjustment based on your Metabase API structure
            query_payload = {
                "type": "native",
                "native": {
                    "query": clean_sql,
                    "template-tags": {}
                },
                "database": self._get_database_id(self.config['mysql_database'])
            }
            
            # Execute query via Metabase API
            query_url = f"{self.config['metabase_url']}/api/dataset"
            logger.info("🚀 Executing MySQL query via Metabase API...")
            
            response = self.metabase_session.post(query_url, json=query_payload)
            response.raise_for_status()
            
            query_result = response.json()
            
            # Convert to DataFrame
            df = self._metabase_result_to_dataframe(query_result)
            logger.info(f"📊 Retrieved {len(df)} records from MySQL database")
            
            # Save to CSV with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"game_data_{timestamp}.csv"
            csv_path = self.csv_files_dir / csv_filename
            
            df.to_csv(csv_path, index=False)
            logger.info(f"💾 Saved MySQL data to: {csv_path}")
            
            # Log sample data for verification
            if len(df) > 0:
                logger.info("📋 Sample data preview:")
                logger.info(f"   Columns: {list(df.columns)}")
                logger.info(f"   First row: {df.iloc[0].to_dict()}")
            
            return str(csv_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch MySQL data: {e}")
            raise
    
    def _get_database_id(self, database_name: str) -> int:
        """Get database ID from Metabase for the given database name"""
        try:
            databases_url = f"{self.config['metabase_url']}/api/database"
            response = self.metabase_session.get(databases_url)
            response.raise_for_status()
            
            databases = response.json()['data']
            for db in databases:
                if db['name'] == database_name:
                    logger.info(f"🔍 Found database '{database_name}' with ID: {db['id']}")
                    return db['id']
            
            raise ValueError(f"Database '{database_name}' not found in Metabase")
            
        except Exception as e:
            logger.error(f"❌ Failed to get database ID for '{database_name}': {e}")
            raise
    
    def _metabase_result_to_dataframe(self, result: Dict) -> pd.DataFrame:
        """Convert Metabase API result to pandas DataFrame"""
        try:
            # Extract column names
            columns = [col['name'] for col in result['data']['cols']]
            
            # Extract rows data
            rows = result['data']['rows']
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            logger.info(f"📊 Converted Metabase result to DataFrame: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to convert Metabase result to DataFrame: {e}")
            raise
    
    def extract_game_ids(self, csv_path: str) -> List[str]:
        """Extract unique game IDs from the CSV file"""
        logger.info("🎮 Extracting game IDs from CSV file...")
        
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"📊 Loaded CSV with {len(df)} records")
            
            # Note: Adjust column name based on your actual CSV structure
            game_id_column = 'game_id'  # TODO: Verify this column name
            
            if game_id_column not in df.columns:
                logger.error(f"❌ Column '{game_id_column}' not found in CSV")
                logger.error(f"Available columns: {list(df.columns)}")
                raise ValueError(f"Game ID column '{game_id_column}' not found")
            
            # Extract unique game IDs
            game_ids = df[game_id_column].dropna().unique().tolist()
            game_ids = [str(gid) for gid in game_ids]  # Ensure string format
            
            logger.info(f"🎯 Extracted {len(game_ids)} unique game IDs")
            logger.info(f"📋 Sample game IDs: {game_ids[:5]}...")
            
            return game_ids
            
        except Exception as e:
            logger.error(f"❌ Failed to extract game IDs: {e}")
            raise
    
    def fetch_registration_details(self, game_ids: List[str]) -> pd.DataFrame:
        """
        Fetch registration details from MongoDB for the given game IDs
        Returns DataFrame with registration_id, registered_time, version, game_id
        """
        logger.info("📝 Fetching registration details from MongoDB via Metabase...")
        
        try:
            # Prepare MongoDB query
            # Note: Adjust field names based on your actual MongoDB structure
            game_ids_str = "', '".join(game_ids)
            mongo_query = f"""
            {{
                "collection": "{self.config['mongodb_collection']}",
                "find": {{
                    "game_id": {{"$in": ["{game_ids_str}"]}}
                }},
                "projection": {{
                    "registration_id": 1,
                    "registered_time": 1,
                    "version": 1,
                    "game_id": 1,
                    "_id": 0
                }}
            }}
            """
            
            # Execute MongoDB query via Metabase
            query_payload = {
                "type": "native",
                "native": {
                    "query": mongo_query,
                    "template-tags": {}
                },
                "database": self._get_database_id(self.config['mongodb_database'])
            }
            
            query_url = f"{self.config['metabase_url']}/api/dataset"
            logger.info("🚀 Executing MongoDB query via Metabase API...")
            
            response = self.metabase_session.post(query_url, json=query_payload)
            response.raise_for_status()
            
            query_result = response.json()
            df = self._metabase_result_to_dataframe(query_result)
            
            logger.info(f"📊 Retrieved {len(df)} registration records from MongoDB")
            
            # Version analysis
            if 'version' in df.columns:
                version_counts = df['version'].value_counts()
                logger.info("📈 Version distribution:")
                for version, count in version_counts.head(10).items():
                    logger.info(f"   Version {version}: {count} registrations")
                
                # Count versions >= 448
                high_version_count = len(df[df['version'] >= self.config['minimum_version']])
                low_version_count = len(df[df['version'] < self.config['minimum_version']])
                
                logger.info(f"🎯 Analysis targets:")
                logger.info(f"   Version >= {self.config['minimum_version']}: {high_version_count} registrations")
                logger.info(f"   Version < {self.config['minimum_version']}: {low_version_count} registrations")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch registration details: {e}")
            raise
    
    def fetch_aws_logs(self, registrations_df: pd.DataFrame) -> Dict[str, str]:
        """
        Fetch AWS logs for registrations with version >= 448
        Returns: Dictionary mapping registration_id to log_file_path
        """
        logger.info("☁️ Fetching AWS logs for registrations...")
        
        # Filter registrations by version
        target_registrations = registrations_df[
            registrations_df['version'] >= self.config['minimum_version']
        ].copy()
        
        logger.info(f"🎯 Targeting {len(target_registrations)} registrations with version >= {self.config['minimum_version']}")
        
        log_files = {}
        
        for idx, row in target_registrations.iterrows():
            registration_id = str(row['registration_id'])
            registered_time = row['registered_time']
            
            try:
                logger.info(f"📥 Fetching logs for registration {registration_id} ({idx+1}/{len(target_registrations)})")
                
                # Convert registered time to IST for S3 path
                ist_datetime = self._convert_to_ist(registered_time)
                s3_prefix = f"rummy_gameplay_logs/{ist_datetime.year:04d}/{ist_datetime.month:02d}/{ist_datetime.day:02d}/"
                
                # Find log files in S3
                log_file_path = self._download_registration_log(registration_id, s3_prefix)
                
                if log_file_path:
                    log_files[registration_id] = log_file_path
                    logger.info(f"✅ Downloaded log for {registration_id}")
                else:
                    logger.warning(f"⚠️ No log found for {registration_id}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to fetch log for {registration_id}: {e}")
                continue
        
        logger.info(f"📁 Successfully downloaded {len(log_files)} log files")
        return log_files
    
    def _convert_to_ist(self, timestamp: str) -> datetime:
        """Convert timestamp to IST"""
        try:
            # Parse timestamp (adjust format based on your data)
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                dt = timestamp
            
            # Add IST offset
            ist_dt = dt + timedelta(
                hours=self.config['ist_offset_hours'],
                minutes=self.config['ist_offset_minutes']
            )
            
            return ist_dt
            
        except Exception as e:
            logger.error(f"❌ Failed to convert timestamp {timestamp}: {e}")
            raise
    
    def _download_registration_log(self, registration_id: str, s3_prefix: str) -> Optional[str]:
        """Download specific registration log from S3"""
        try:
            # List objects in S3 with the given prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.config['aws_s3_bucket'],
                Prefix=s3_prefix
            )
            
            if 'Contents' not in response:
                return None
            
            # Look for zip files
            zip_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.zip')]
            
            for zip_file in zip_files:
                # Download and check if it contains the registration ID
                local_zip_path = self.logs_files_dir / f"temp_{registration_id}.zip"
                
                self.s3_client.download_file(
                    self.config['aws_s3_bucket'],
                    zip_file,
                    str(local_zip_path)
                )
                
                # Extract and search for registration ID
                extract_dir = self.logs_files_dir / f"temp_extract_{registration_id}"
                extract_dir.mkdir(exist_ok=True)
                
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                # Search for registration ID in extracted files
                for extracted_file in extract_dir.rglob('*.log'):
                    if self._search_registration_in_file(str(extracted_file), registration_id):
                        # Found the log file
                        final_log_path = self.logs_files_dir / f"{registration_id}_logs.log"
                        extracted_file.rename(final_log_path)
                        
                        # Cleanup
                        local_zip_path.unlink()
                        import shutil
                        shutil.rmtree(extract_dir)
                        
                        return str(final_log_path)
                
                # Cleanup if not found
                local_zip_path.unlink()
                import shutil
                shutil.rmtree(extract_dir)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error downloading log for {registration_id}: {e}")
            return None
    
    def _search_registration_in_file(self, file_path: str, registration_id: str) -> bool:
        """Search for registration ID in log file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                return registration_id in content
        except Exception:
            return False
    
    def analyze_matchmaking_failures(self, log_files: Dict[str, str], registrations_df: pd.DataFrame) -> Dict:
        """
        Analyze matchmaking failures using the cursor rule
        Returns comprehensive analysis results
        """
        logger.info("🔬 Analyzing matchmaking failures using cursor rule...")
        
        analysis_results = {
            'total_logs_analyzed': len(log_files),
            'version_analysis': {
                'high_version_failures': {},  # >= 448
                'low_version_failures': {},   # < 448
            },
            'failure_categories': {
                'phase1_registration_failures': [],
                'phase2_table_assignment_failures': [],
                'phase3_socket_connection_failures': [],
                'phase4_matchmaking_failures': [],
                'unknown_failures': []
            },
            'detailed_results': []
        }
        
        # Analyze each log file
        for registration_id, log_file_path in log_files.items():
            logger.info(f"🔍 Analyzing {registration_id}...")
            
            try:
                # Get version for this registration
                reg_info = registrations_df[registrations_df['registration_id'] == registration_id].iloc[0]
                version = reg_info['version']
                
                # Perform cursor rule analysis
                failure_analysis = self._analyze_single_log_with_cursor_rule(log_file_path, registration_id)
                failure_analysis['version'] = version
                failure_analysis['registration_id'] = registration_id
                
                # Categorize by version
                version_category = 'high_version_failures' if version >= self.config['minimum_version'] else 'low_version_failures'
                
                if failure_analysis['failure_point'] not in analysis_results['version_analysis'][version_category]:
                    analysis_results['version_analysis'][version_category][failure_analysis['failure_point']] = 0
                analysis_results['version_analysis'][version_category][failure_analysis['failure_point']] += 1
                
                # Categorize by failure phase
                failure_phase = failure_analysis['failure_point'].lower()
                if 'registration' in failure_phase:
                    analysis_results['failure_categories']['phase1_registration_failures'].append(failure_analysis)
                elif 'table' in failure_phase or 'assignment' in failure_phase:
                    analysis_results['failure_categories']['phase2_table_assignment_failures'].append(failure_analysis)
                elif 'socket' in failure_phase or 'connection' in failure_phase:
                    analysis_results['failure_categories']['phase3_socket_connection_failures'].append(failure_analysis)
                elif 'matchmaking' in failure_phase:
                    analysis_results['failure_categories']['phase4_matchmaking_failures'].append(failure_analysis)
                else:
                    analysis_results['failure_categories']['unknown_failures'].append(failure_analysis)
                
                analysis_results['detailed_results'].append(failure_analysis)
                
            except Exception as e:
                logger.error(f"❌ Failed to analyze {registration_id}: {e}")
                continue
        
        # Generate summary statistics
        self._generate_analysis_summary(analysis_results)
        
        return analysis_results
    
    def _analyze_single_log_with_cursor_rule(self, log_file_path: str, registration_id: str) -> Dict:
        """
        Analyze single log file using the comprehensive cursor rule
        This implements the exact phases from the cursor rule
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
                        
                        analysis_result["failure_point"] = phase4_result.get("failure_point", "MATCHMAKING_UNKNOWN")
                        analysis_result["failure_type"] = phase4_result.get("failure_type", "UNKNOWN_MATCHMAKING_FAILURE")
                    else:
                        analysis_result["failure_point"] = "SOCKET_CONNECTION"
                        analysis_result["failure_type"] = "NETWORK_FAILURE"
                else:
                    analysis_result["failure_point"] = "TABLE_ASSIGNMENT"
                    analysis_result["failure_type"] = "ALLOCATION_FAILURE"
            else:
                analysis_result["failure_point"] = "REGISTRATION"
                analysis_result["failure_type"] = "REGISTRATION_FAILURE"
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing log {log_file_path}: {e}")
            analysis_result["error"] = str(e)
            return analysis_result
    
    def _analyze_phase1_registration(self, log_content: str, registration_id: str) -> Dict:
        """Phase 1: Tournament Registration Verification (from cursor rule)"""
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
        """Phase 2: Game Table Assignment Verification (from cursor rule)"""
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
        """Phase 3: Gameplay Socket Connection Verification (from cursor rule)"""
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
        """Phase 4: Matchmaking Lifecycle Analysis (from cursor rule)"""
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
                result["failure_point"] = "NO_FAILURE"
                result["failure_type"] = "SUCCESS"
            elif failed_matches:
                result["status"] = "FAILED"
                result["details"]["outcome"] = "SERVER_SIDE_FAILURE"
                result["failure_point"] = "MATCHMAKING_LOGIC"
                result["failure_type"] = "SERVER_SIDE_MATCHMAKING_FAILURE"
            elif timeout_matches:
                result["status"] = "FAILED"
                result["details"]["outcome"] = "CLIENT_SIDE_TIMEOUT"
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
            result["failure_point"] = "QUEUE_ENTRY"
            result["failure_type"] = "QUEUE_ENTRY_FAILURE"
        
        return result
    
    def _generate_analysis_summary(self, analysis_results: Dict):
        """Generate and log comprehensive analysis summary"""
        logger.info("\n" + "="*80)
        logger.info("📊 COMPREHENSIVE MATCHMAKING FAILURE ANALYSIS SUMMARY")
        logger.info("="*80)
        
        total = analysis_results['total_logs_analyzed']
        logger.info(f"📈 Total Logs Analyzed: {total}")
        
        # Version-based analysis
        logger.info("\n🔢 VERSION-BASED FAILURE ANALYSIS:")
        logger.info("-" * 50)
        
        high_version_failures = analysis_results['version_analysis']['high_version_failures']
        low_version_failures = analysis_results['version_analysis']['low_version_failures']
        
        logger.info(f"📱 Version >= {self.config['minimum_version']} Failures:")
        total_high = sum(high_version_failures.values())
        for failure_point, count in high_version_failures.items():
            percentage = (count / total_high * 100) if total_high > 0 else 0
            logger.info(f"   ├── {failure_point}: {count} ({percentage:.1f}%)")
        
        logger.info(f"📱 Version < {self.config['minimum_version']} Failures:")
        total_low = sum(low_version_failures.values())
        for failure_point, count in low_version_failures.items():
            percentage = (count / total_low * 100) if total_low > 0 else 0
            logger.info(f"   ├── {failure_point}: {count} ({percentage:.1f}%)")
        
        # Phase-based analysis
        logger.info("\n🔍 PHASE-BASED FAILURE ANALYSIS:")
        logger.info("-" * 50)
        
        for phase, failures in analysis_results['failure_categories'].items():
            if failures:
                logger.info(f"🎯 {phase.replace('_', ' ').title()}: {len(failures)} cases")
                
                # Show breakdown by version for each phase
                version_breakdown = {}
                for failure in failures:
                    version = failure.get('version', 'unknown')
                    version_key = f">={self.config['minimum_version']}" if version >= self.config['minimum_version'] else f"<{self.config['minimum_version']}"
                    version_breakdown[version_key] = version_breakdown.get(version_key, 0) + 1
                
                for version_range, count in version_breakdown.items():
                    logger.info(f"   ├── Version {version_range}: {count} cases")
        
        logger.info("="*80)
    
    def generate_final_report(self, analysis_results: Dict, csv_path: str):
        """Generate final comprehensive report and save to files"""
        logger.info("📋 Generating final comprehensive report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("analysis_reports")
        report_dir.mkdir(exist_ok=True)
        
        # Generate JSON report
        json_report_path = report_dir / f"matchmaking_analysis_{timestamp}.json"
        with open(json_report_path, 'w') as f:
            json.dump(analysis_results, f, indent=2, default=str)
        
        # Generate human-readable report
        txt_report_path = report_dir / f"matchmaking_analysis_{timestamp}.txt"
        with open(txt_report_path, 'w') as f:
            f.write("COMPREHENSIVE MATCHMAKING FAILURE ANALYSIS REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Analysis Timestamp: {datetime.now()}\n")
            f.write(f"Total Logs Analyzed: {analysis_results['total_logs_analyzed']}\n")
            f.write(f"Source CSV: {csv_path}\n\n")
            
            # Version analysis
            f.write("VERSION-BASED ANALYSIS:\n")
            f.write("-" * 40 + "\n")
            
            high_version = analysis_results['version_analysis']['high_version_failures']
            low_version = analysis_results['version_analysis']['low_version_failures']
            
            f.write(f"Version >= {self.config['minimum_version']} Failures:\n")
            for failure_point, count in high_version.items():
                f.write(f"  - {failure_point}: {count}\n")
            
            f.write(f"\nVersion < {self.config['minimum_version']} Failures:\n")
            for failure_point, count in low_version.items():
                f.write(f"  - {failure_point}: {count}\n")
            
            # Phase analysis
            f.write("\nPHASE-BASED ANALYSIS:\n")
            f.write("-" * 40 + "\n")
            
            for phase, failures in analysis_results['failure_categories'].items():
                if failures:
                    f.write(f"{phase.replace('_', ' ').title()}: {len(failures)} cases\n")
        
        logger.info(f"📄 JSON Report saved: {json_report_path}")
        logger.info(f"📄 Text Report saved: {txt_report_path}")
        
        return json_report_path, txt_report_path
    
    def run_complete_analysis(self):
        """Run the complete end-to-end analysis pipeline"""
        try:
            logger.info("🚀 STARTING COMPLETE METABASE TO AWS ANALYSIS PIPELINE")
            logger.info("="*80)
            
            # Step 1: Get user input
            print("\n🎯 STEP 1: Getting analysis time period from user")
            start_time, end_time = self.get_user_input()
            
            # Step 2: Fetch MySQL data
            print("\n🗃️ STEP 2: Fetching game data from MySQL via Metabase")
            csv_path = self.fetch_mysql_data(start_time, end_time)
            
            # Step 3: Extract game IDs
            print("\n🎮 STEP 3: Extracting game IDs from CSV")
            game_ids = self.extract_game_ids(csv_path)
            
            # Step 4: Fetch registration details
            print("\n📝 STEP 4: Fetching registration details from MongoDB")
            registrations_df = self.fetch_registration_details(game_ids)
            
            # Step 5: Fetch AWS logs
            print("\n☁️ STEP 5: Fetching AWS logs for version >= 448")
            log_files = self.fetch_aws_logs(registrations_df)
            
            # Step 6: Analyze matchmaking failures
            print("\n🔬 STEP 6: Analyzing matchmaking failures using cursor rule")
            analysis_results = self.analyze_matchmaking_failures(log_files, registrations_df)
            
            # Step 7: Generate final report
            print("\n📋 STEP 7: Generating comprehensive final report")
            json_report, txt_report = self.generate_final_report(analysis_results, csv_path)
            
            # Final summary
            print("\n" + "="*80)
            print("🎉 ANALYSIS PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"📊 Total registrations analyzed: {len(log_files)}")
            print(f"📄 Reports generated:")
            print(f"   - JSON: {json_report}")
            print(f"   - Text: {txt_report}")
            print(f"💾 Files saved in:")
            print(f"   - CSV files: {self.csv_files_dir}")
            print(f"   - Log files: {self.logs_files_dir}")
            print("="*80)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️ Analysis interrupted by user")
            sys.exit(1)
        except Exception as e:
            logger.error(f"\n❌ Analysis pipeline failed: {e}")
            raise

def main():
    """Main function to run the complete analysis pipeline"""
    try:
        # Check environment file
        if not os.path.exists('.env'):
            print("❌ .env file not found!")
            print("💡 Please create a .env file with required credentials")
            print("📝 Refer to the README for required environment variables")
            sys.exit(1)
        
        # Create and run analyzer
        analyzer = MetabaseToAWSAnalyzer()
        analyzer.run_complete_analysis()
        
    except Exception as e:
        logger.error(f"❌ Failed to run analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 