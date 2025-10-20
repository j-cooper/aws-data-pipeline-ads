#!/bin/bash

echo "=================================================="
echo "📦 PREPARING LAMBDA DEPLOYMENT PACKAGE"
echo "=================================================="

# Configuration
FUNCTION_NAME="data-pipeline-etl"
PACKAGE_FILE="lambda_function.zip"

# Clean up old package
echo -e "\n🧹 Cleaning up old files..."
rm -f $PACKAGE_FILE
rm -rf lambda_package/

# Create package directory
echo -e "\n📁 Creating package directory..."
mkdir -p lambda_package

# Copy Lambda function
echo -e "\n📝 Copying Lambda function..."
cp lambda/lambda_function.py lambda_package/

# Install dependencies
echo -e "\n📚 Installing dependencies..."
pip install --target lambda_package/ urllib3 > /dev/null 2>&1

# Create ZIP file
echo -e "\n🗜️ Creating ZIP package..."
cd lambda_package
zip -r ../$PACKAGE_FILE . -q
cd ..

# Get package size
PACKAGE_SIZE=$(ls -lh $PACKAGE_FILE | awk '{print $5}')

# Clean up temp directory
rm -rf lambda_package/

echo -e "\n✅ Package created successfully!"
echo "   File: $PACKAGE_FILE"
echo "   Size: $PACKAGE_SIZE"
echo ""
echo "=================================================="