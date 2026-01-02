#!/bin/bash

set -e

# ================================
# Configuration
# ================================
BUCKET_NAME="weather-data-lake-caio"
AWS_REGION="us-east-1"

echo "🚀 Starting S3 bucket setup..."
echo "Bucket: $BUCKET_NAME"
echo "Region: $AWS_REGION"
echo "-------------------------------"

# ================================
# Check if bucket already exists
# ================================
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
  echo "✅ Bucket already exists: $BUCKET_NAME"
else
  echo "🪣 Creating bucket: $BUCKET_NAME"
  aws s3api create-bucket \
    --bucket "$BUCKET_NAME" \
    --region "$AWS_REGION"

  echo "✅ Bucket created successfully"
fi

# ================================
# Create Bronze / Silver / Gold
# ================================
echo "📂 Creating data lake structure..."

aws s3api put-object --bucket "$BUCKET_NAME" --key bronze/
aws s3api put-object --bucket "$BUCKET_NAME" --key silver/
aws s3api put-object --bucket "$BUCKET_NAME" --key gold/

echo "✅ Data lake folders created:"
echo "  - bronze/"
echo "  - silver/"
echo "  - gold/"

# ================================
# Final check
# ================================
echo "🔍 Verifying bucket structure..."
aws s3 ls "s3://$BUCKET_NAME/"

echo "🎉 S3 data lake setup completed successfully!"
