# 🚀 Automated Matchmaking Failure Analyzer

This script automates the complete process of analyzing matchmaking failures by:
1. Reading CSV files with registration IDs and timestamps
2. Converting GMT to IST timezone (+5:30)
3. Downloading corresponding log files from AWS S3
4. Extracting and analyzing logs using comprehensive matchmaking diagnosis
5. Generating detailed failure analysis reports

## 📋 Prerequisites

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS Credentials
You need AWS credentials configured. Choose one method:

**Option A: AWS CLI (Recommended)**
```bash
aws configure
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=ap-south-1
```

**Option C: IAM Role (if running on EC2)**
- Attach appropriate IAM role with S3 read permissions

### 3. Required S3 Permissions
Your AWS credentials need the following permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::prod-rummy-shared-upload-m-bucket",
                "arn:aws:s3:::prod-rummy-shared-upload-m-bucket/*"
            ]
        }
    ]
}
```

## 📊 CSV File Format

Your input CSV file must contain at least these columns:

| Column | Description | Example |
|--------|-------------|---------|
| `registrationId` | Unique registration ID | `687e224d65aa1712f6e06083` |
| `created_at` | GMT timestamp | `July 27, 2025, 11:52:11.000 AM` |

### Sample CSV Content:
```csv
registrationId,created_at,reason
687e224d65aa1712f6e06083,"July 27, 2025, 11:52:11.000 AM",matchmaking-failed
687e2209cac29f92289f44d3,"July 27, 2025, 04:46:00.928 PM",matchmaking-failed
```

## 🛠️ Usage

### Basic Usage
```bash
python automated_matchmaking_analyzer.py --csv failures.csv
```

### Advanced Usage with Custom Options
```bash
python automated_matchmaking_analyzer.py \
    --csv matchmaking_failures.csv \
    --output-dir ./analysis_results \
    --bucket prod-rummy-shared-upload-m-bucket \
    --region ap-south-1 \
    --verbose
```

### Command Line Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--csv` | ✅ Yes | - | Path to CSV file containing matchmaking failures |
| `--output-dir` | ❌ No | `./analysis_output` | Output directory for analysis results |
| `--bucket` | ❌ No | `prod-rummy-shared-upload-m-bucket` | S3 bucket name |
| `--region` | ❌ No | `ap-south-1` | AWS region |
| `--verbose` | ❌ No | `False` | Enable verbose logging |

## 📁 Output Structure

After running the script, you'll get:

```
analysis_output/
├── analysis_summary.json          # Overall analysis summary
├── analysis_report.txt            # Human-readable report
├── {registrationId}_analysis.json # Individual analysis results
├── {registrationId}_extracted/    # Extracted log files
└── matchmaking_analyzer.log       # Execution log
```

## 📊 Analysis Results

### Individual Analysis Files
Each `{registrationId}_analysis.json` contains:
- **Phase-by-phase analysis** following the comprehensive diagnosis rule
- **Failure point identification** (Registration, Table Assignment, Socket Connection, Matchmaking)
- **Failure type classification** (Server-side, Client-side, Network, etc.)
- **Actionable recommendations**

### Summary Report
The `analysis_summary.json` and `analysis_report.txt` provide:
- **Success rate** of analysis processing
- **Failure point distribution** across all cases
- **Failure type patterns**
- **Top recommendations** ranked by frequency

## 🔧 Troubleshooting

### Common Issues

**1. AWS Credentials Error**
```
❌ Failed to connect to AWS S3: NoCredentialsError
```
**Solution**: Configure AWS credentials using `aws configure`

**2. S3 Access Denied**
```
❌ Error listing S3 objects: AccessDenied
```
**Solution**: Ensure your AWS credentials have S3 read permissions

**3. No Log Files Found**
```
⚠️ No log files found for {registrationId} on {date}
```
**Solution**: 
- Verify the date conversion (GMT → IST +5:30)
- Check if logs exist for that specific date in S3
- Ensure the S3 path structure matches: `rummy_gameplay_logs/YYYY/MM/DD/`

**4. Registration ID Not Found in Logs**
```
⚠️ Registration ID {id} not found in any log files
```
**Solution**:
- The registration ID might be in a different time bucket
- Check if the timestamp conversion is accurate
- Verify the registration ID format

## 🎯 Analysis Phases Explained

The script follows the **Comprehensive Rummy Matchmaking Failure Diagnosis** rule:

### Phase 1: Tournament Registration Verification
- ✅ Checks for `registerTournament` API request
- ✅ Verifies successful response with `registrationId`

### Phase 2: Game Table Assignment Verification  
- ✅ Looks for `getTournamentDetails` API call
- ✅ Confirms `TABLE_ASSIGNED` status and game server assignment

### Phase 3: Gameplay Socket Connection Verification
- ✅ Finds socket connection attempts
- ✅ Verifies successful connection or identifies failures

### Phase 4: Matchmaking Lifecycle Analysis
- ✅ Tracks user entering `FINDING` state
- ✅ Identifies final outcome:
  - `ROUND_STARTING` (Success)
  - `MATCH_MAKING_FAILED` (Server-side failure)
  - `backToLobbyInterval Timer expired` (Client timeout)

## 📈 Sample Analysis Output

```json
{
  "registration_id": "687e224d65aa1712f6e06083",
  "failure_point": "MATCHMAKING_LOGIC",
  "failure_type": "SERVER_SIDE_MATCHMAKING_FAILURE",
  "phases": {
    "phase1_registration": {"status": "SUCCESS"},
    "phase2_table_assignment": {"status": "SUCCESS"},
    "phase3_socket_connection": {"status": "SUCCESS"},
    "phase4_matchmaking_lifecycle": {"status": "FAILED"}
  },
  "recommendations": [
    "Investigate game server matchmaking algorithm",
    "Check for race conditions or deadlocks",
    "Examine specific pod IP: 10.200.248.216"
  ]
}
```

## 🚀 Getting Started

1. **Prepare your CSV file** with registration IDs and timestamps
2. **Configure AWS credentials**: `aws configure`
3. **Install dependencies**: `pip install -r requirements.txt`
4. **Run the analyzer**: `python automated_matchmaking_analyzer.py --csv your_file.csv`
5. **Review results** in the `analysis_output` directory

## 💡 Tips for Best Results

1. **Use accurate timestamps** - GMT to IST conversion is critical
2. **Check S3 permissions** - Ensure you can read from the target bucket
3. **Monitor progress** - Use `--verbose` flag for detailed logging
4. **Batch processing** - Process multiple failures in one CSV for pattern analysis
5. **Regular cleanup** - The script downloads and extracts files locally

## 🆘 Support

For issues or questions:
1. Check the `matchmaking_analyzer.log` file for detailed error messages
2. Verify your CSV format matches the expected structure
3. Ensure AWS credentials and permissions are correctly configured
4. Test with a small sample CSV first before processing large batches 