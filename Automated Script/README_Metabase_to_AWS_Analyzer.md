# 🚀 Metabase to AWS Analyzer - Complete Pipeline

This script provides an **end-to-end automated analysis pipeline** for debugging matchmaking failures by connecting Metabase databases to AWS logs and applying comprehensive failure analysis.

## 🎯 **What This Script Does**

### **Complete Pipeline Flow:**
```
User Input (Date/Time) → Metabase MySQL → CSV → Game IDs → Metabase MongoDB → 
Registration IDs → AWS S3 Logs → Cursor Rule Analysis → Failure Report
```

### **Key Features:**
1. **📊 Fetches game data** from Metabase MySQL database using custom SQL
2. **🎮 Extracts game IDs** and cross-references with MongoDB registrations  
3. **🔍 Filters by version** (analyzes version >= 448 vs < 448)
4. **☁️ Downloads AWS logs** for targeted registration IDs
5. **🔬 Applies cursor rule analysis** for comprehensive failure categorization
6. **📋 Generates detailed reports** with version-based and phase-based breakdowns

---

## 📋 **Prerequisites**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Configure Environment Variables**
```bash
# Copy the environment template
cp "Automated Script/sample_env_metabase.env" .env

# Edit with your actual credentials
nano .env
```

### **3. Required Access**
- ✅ **Metabase API access** (API key or username/password)
- ✅ **AWS S3 read permissions** for log bucket
- ✅ **Database access** via Metabase (MySQL + MongoDB)

---

## ⚙️ **Environment Configuration**

### **🔐 Required Secrets in .env:**

```bash
# Metabase Connection
METABASE_URL=https://your-metabase-instance.com
METABASE_API_KEY=your_api_key_here

# Database Names  
MYSQL_DATABASE_NAME=MysqlRummyGameplay
MONGODB_DATABASE_NAME=MongoDB Main
MONGODB_COLLECTION_NAME=Registrations

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_S3_BUCKET=prod-rummy-shared-upload-m-bucket

# Analysis Settings
MINIMUM_VERSION_ANALYSIS=448
```

### **🏗️ Database Structure Requirements**

**MySQL (MysqlRummyGameplay):**
- Uses SQL query from `SQL queries/fetch_data_query.sql`
- Expected columns: `game_id`, `table_id`, `created_at`, etc.

**MongoDB (MongoDB Main/Registrations):**
- Expected fields: `registration_id`, `registered_time`, `version`, `game_id`

---

## 🚀 **Usage**

### **Basic Usage**
```bash
python "Automated Script/Analyzer_from_metabase_to_AWS.py"
```

### **Interactive Flow**
The script will prompt you for:
1. **Start Date** (YYYY-MM-DD)
2. **Start Time** (HH:MM)  
3. **End Date** (YYYY-MM-DD, optional)
4. **End Time** (HH:MM)

### **Example Session**
```
🎯 METABASE TO AWS ANALYZER - INPUT REQUIRED
============================================
📅 Enter the analysis time period:
Start Date (YYYY-MM-DD): 2025-08-01
Start Time (HH:MM): 10:00
End Date (YYYY-MM-DD, press Enter for same day): 2025-08-01
End Time (HH:MM): 16:00

✅ Analysis Period: 2025-08-01 10:00:00 to 2025-08-01 16:00:00
Confirm this time period? (y/N): y
```

---

## 📁 **Output Structure**

### **Generated Files:**
```
CSV files/
├── game_data_20250801_140532.csv          # MySQL query results

logs files/  
├── 687e224d65aa1712f6e06083_logs.log       # Individual log files
├── 687e2209cac29f92289f44d3_logs.log
└── ...

analysis_reports/
├── matchmaking_analysis_20250801_140532.json    # Detailed JSON report
└── matchmaking_analysis_20250801_140532.txt     # Human-readable report
```

### **Console Output:**
```
🚀 STARTING COMPLETE METABASE TO AWS ANALYSIS PIPELINE
🗃️ STEP 1: Fetching game data from MySQL via Metabase
📊 Retrieved 1,247 records from MySQL database
🎮 STEP 2: Extracting game IDs from CSV  
🎯 Extracted 892 unique game IDs
📝 STEP 3: Fetching registration details from MongoDB
📊 Retrieved 1,156 registration records from MongoDB
📈 Version distribution:
   Version 448: 423 registrations
   Version 447: 298 registrations
   Version 449: 201 registrations
☁️ STEP 4: Fetching AWS logs for version >= 448
📁 Successfully downloaded 387 log files
🔬 STEP 5: Analyzing matchmaking failures using cursor rule
```

---

## 📊 **Analysis Results**

### **Version-Based Analysis**
The script categorizes failures by app version:

```
📱 Version >= 448 Failures:
   ├── MATCHMAKING_LOGIC: 145 (65.6%)
   ├── SOCKET_CONNECTION: 42 (19.0%)  
   ├── TABLE_ASSIGNMENT: 21 (9.5%)
   ├── REGISTRATION: 13 (5.9%)

📱 Version < 448 Failures:
   ├── SOCKET_CONNECTION: 89 (71.2%)
   ├── MATCHMAKING_LOGIC: 23 (18.4%)
   ├── TABLE_ASSIGNMENT: 8 (6.4%)
   ├── REGISTRATION: 5 (4.0%)
```

### **Phase-Based Analysis**
Following the comprehensive cursor rule:

1. **Phase 1: Registration Failures** - API call failures, wallet issues
2. **Phase 2: Table Assignment Failures** - Server allocation problems  
3. **Phase 3: Socket Connection Failures** - Network connectivity issues
4. **Phase 4: Matchmaking Failures** - Server-side logic problems

---

## 🔍 **Cursor Rule Analysis**

The script implements the **complete cursor rule** for matchmaking diagnosis:

### **Phase 1: Tournament Registration Verification**
- ✅ Checks `/v1.0/super/tournament/registerTournament` API calls
- ✅ Verifies successful response with `registrationId`

### **Phase 2: Game Table Assignment Verification**
- ✅ Looks for `/v1.0/super/tournament/getTournamentDetails` calls
- ✅ Confirms `TABLE_ASSIGNED` status

### **Phase 3: Socket Connection Verification**  
- ✅ Tracks socket connection attempts and results
- ✅ Identifies network vs server-side connection failures

### **Phase 4: Matchmaking Lifecycle Analysis**
- ✅ Monitors `FINDING` state entry
- ✅ Categorizes final outcomes:
  - `ROUND_STARTING` (Success)
  - `MATCH_MAKING_FAILED` (Server failure)  
  - `backToLobbyInterval Timer expired` (Client timeout)

---

## 🔧 **Troubleshooting**

### **Common Issues**

**1. Metabase Connection Errors**
```
❌ Failed to initialize Metabase connection: 401 Unauthorized
```
**Solution:** Check your `METABASE_API_KEY` or `METABASE_USERNAME`/`METABASE_PASSWORD`

**2. Database Not Found**
```
❌ Database 'MysqlRummyGameplay' not found in Metabase
```
**Solution:** Verify database names in Metabase match your `.env` configuration

**3. SQL Query Errors**
```
❌ Failed to fetch MySQL data: syntax error
```
**Solution:** Check `SQL queries/fetch_data_query.sql` for correct syntax

**4. AWS S3 Access Issues**
```
❌ Failed to initialize AWS connection: NoCredentialsError
```
**Solution:** Verify AWS credentials and S3 bucket permissions

**5. No Logs Found**
```
⚠️ No log found for registration 687e224d65aa1712f6e06083
```
**Solution:** 
- Check if logs exist for that date in S3
- Verify timestamp conversion (GMT → IST)
- Ensure registration occurred within the log retention period

---

## 📈 **Performance Optimization**

### **Configuration Options:**
```bash
# Limit parallel requests to avoid API rate limits
MAX_PARALLEL_REQUESTS=5

# Limit log files per registration to save bandwidth  
MAX_LOG_FILES_PER_REGISTRATION=3

# Timeout for analysis operations
ANALYSIS_TIMEOUT_SECONDS=1800
```

### **Best Practices:**
1. **Start with smaller date ranges** for initial testing
2. **Use API keys** instead of username/password for Metabase
3. **Monitor logs** with `LOG_LEVEL=DEBUG` for detailed diagnostics
4. **Clean up temporary files** regularly from `logs files/` directory

---

## 🎯 **Understanding the Output**

### **Key Metrics to Monitor:**

**Version >= 448 Analysis:**
- Focus on new matchmaking logic effectiveness
- Monitor for regressions in latest app versions
- Track socket connection stability improvements

**Version < 448 Analysis:**  
- Baseline comparison for legacy behavior
- Identify patterns that persist across versions
- Historical trend analysis

**Cross-Version Patterns:**
- Network infrastructure issues (affect all versions)
- Server capacity problems (version-independent)
- Client-specific problems (version-dependent)

---

## 💡 **Advanced Usage Tips**

### **1. Custom Time Ranges**
- Use **specific hours** for peak traffic analysis
- **Multi-day ranges** for trend analysis
- **Timezone awareness** - all times converted to IST automatically

### **2. Data Interpretation**
- **High socket failures** → Network/infrastructure issues
- **High matchmaking failures** → Server logic or liquidity problems  
- **High registration failures** → User authentication/wallet issues

### **3. Report Analysis**
- **JSON reports** → Machine-readable for further processing
- **Text reports** → Human-readable executive summaries
- **Console logs** → Real-time monitoring during execution

---

## 🆘 **Support & Questions**

### **Before Running:**
1. ✅ Verify `.env` file has all required credentials
2. ✅ Test Metabase API access manually
3. ✅ Confirm AWS S3 bucket access
4. ✅ Check `SQL queries/fetch_data_query.sql` syntax

### **Common Questions:**

**Q: How long does the analysis take?**
A: Depends on data volume. Typically 10-30 minutes for a day's worth of data.

**Q: Can I analyze multiple days at once?**  
A: Yes, but be mindful of API rate limits and storage space for logs.

**Q: What if some registrations don't have logs?**
A: Normal - logs may not exist due to retention policies or incomplete sessions.

**Q: How accurate is the cursor rule analysis?**
A: Very high accuracy for structured failures. Unknown cases require manual investigation.

---

## 🔄 **Regular Maintenance**

### **Weekly Tasks:**
- Clean up old CSV files from `CSV files/`
- Archive old logs from `logs files/`  
- Review analysis reports for patterns

### **Monthly Tasks:**
- Rotate API keys and AWS credentials
- Update minimum version threshold as app versions advance
- Review and optimize SQL queries for performance

---

**🎉 This analyzer provides comprehensive insights into matchmaking failures, enabling data-driven decisions for improving user experience and system reliability.** 