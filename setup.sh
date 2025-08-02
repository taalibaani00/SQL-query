#!/bin/bash

# 🚀 Automated Matchmaking Analyzer Setup Script

echo "🔧 Setting up Automated Matchmaking Analyzer..."
echo "================================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.7+ first."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Install dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ Dependencies installed successfully"
else
    echo "❌ Failed to install dependencies"
    exit 1
fi

# Check if AWS CLI is installed
if command -v aws &> /dev/null; then
    echo "✅ AWS CLI found: $(aws --version)"
    
    # Check if AWS credentials are configured
    if aws sts get-caller-identity &> /dev/null; then
        echo "✅ AWS credentials are configured"
    else
        echo "⚠️  AWS credentials not configured. Please run 'aws configure'"
        echo "   You'll need:"
        echo "   - AWS Access Key ID"
        echo "   - AWS Secret Access Key"
        echo "   - Default region: ap-south-1"
    fi
else
    echo "⚠️  AWS CLI not found. Install it from: https://aws.amazon.com/cli/"
    echo "   Or configure credentials using environment variables:"
    echo "   export AWS_ACCESS_KEY_ID=your_access_key"
    echo "   export AWS_SECRET_ACCESS_KEY=your_secret_key"
    echo "   export AWS_DEFAULT_REGION=ap-south-1"
fi

# Create output directory
mkdir -p analysis_output
echo "📁 Created analysis_output directory"

echo ""
echo "🎉 Setup completed!"
echo "================================================"
echo ""
echo "📚 Next steps:"
echo "1. Prepare your CSV file with matchmaking failures"
echo "2. Configure AWS credentials (if not done already)"
echo "3. Run the analyzer:"
echo "   python3 automated_matchmaking_analyzer.py --csv your_file.csv"
echo ""
echo "📖 For detailed instructions, see README_automated_analyzer.md"
echo "📋 Sample CSV format available in sample_failures.csv" 