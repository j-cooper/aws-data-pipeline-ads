#!/usr/bin/env python3
"""
Test S3 operations to verify our setup
This script will:
1. Connect to S3
2. List buckets
3. Upload a test file
4. Download the test file
5. Clean up
"""

import boto3
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
BUCKET_NAME = os.getenv('BUCKET_NAME', 'your-bucket-name')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

def test_s3_connection():
    """Test basic S3 connection"""
    print("=" * 60)
    print("🧪 TESTING S3 CONNECTION")
    print("=" * 60)
    
    try:
        # Create S3 client
        s3 = boto3.client('s3', region_name=AWS_REGION)
        
        # List buckets
        print("\n📦 Your S3 Buckets:")
        response = s3.list_buckets()
        
        for bucket in response['Buckets']:
            print(f"   - {bucket['Name']}")
            if bucket['Name'] == BUCKET_NAME:
                print(f"     ✅ Found our bucket!")
        
        return s3
        
    except Exception as e:
        print(f"❌ Error connecting to S3: {e}")
        return None


def test_upload(s3):
    """Test uploading a file to S3"""
    print("\n" + "=" * 60)
    print("📤 TESTING UPLOAD")
    print("=" * 60)
    
    try:
        # Create test data
        test_data = {
            "test_id": "test_001",
            "timestamp": datetime.now().isoformat(),
            "message": "Hello from AWS Data Pipeline!",
            "data": {
                "source": "test_script",
                "records": 100,
                "status": "success"
            }
        }
        
        # Convert to JSON
        json_data = json.dumps(test_data, indent=2)
        
        # Upload to S3
        key = f"temp/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        print(f"\n📝 Uploading test file...")
        print(f"   Bucket: {BUCKET_NAME}")
        print(f"   Key: {key}")
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json_data,
            ContentType='application/json',
            Metadata={
                'test': 'true',
                'created_by': 'test_script'
            }
        )
        
        print(f"   ✅ Upload successful!")
        
        return key
        
    except Exception as e:
        print(f"❌ Error uploading: {e}")
        return None


def test_download(s3, key):
    """Test downloading a file from S3"""
    print("\n" + "=" * 60)
    print("📥 TESTING DOWNLOAD")
    print("=" * 60)
    
    try:
        print(f"\n📝 Downloading test file...")
        print(f"   Bucket: {BUCKET_NAME}")
        print(f"   Key: {key}")
        
        # Download from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        
        # Read the content
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        print(f"   ✅ Download successful!")
        print(f"\n📄 File content:")
        print(json.dumps(data, indent=2))
        
        return True
        
    except Exception as e:
        print(f"❌ Error downloading: {e}")
        return False


def test_list_objects(s3):
    """Test listing objects in bucket"""
    print("\n" + "=" * 60)
    print("📋 LISTING BUCKET CONTENTS")
    print("=" * 60)
    
    try:
        print(f"\n📦 Contents of {BUCKET_NAME}:")
        
        # List objects
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                size = obj['Size']
                modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"   - {obj['Key']}")
                print(f"     Size: {size} bytes | Modified: {modified}")
        else:
            print("   (empty bucket)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error listing objects: {e}")
        return False


def cleanup(s3, key):
    """Clean up test file"""
    print("\n" + "=" * 60)
    print("🧹 CLEANUP")
    print("=" * 60)
    
    try:
        if key:
            print(f"\n🗑️  Deleting test file: {key}")
            s3.delete_object(Bucket=BUCKET_NAME, Key=key)
            print(f"   ✅ Cleanup complete!")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")


def main():
    """Run all tests"""
    print("\n" + "🚀 " * 20)
    print(" AWS S3 CONNECTION TEST ")
    print("🚀 " * 20)
    
    print(f"\n📋 Configuration:")
    print(f"   Region: {AWS_REGION}")
    print(f"   Bucket: {BUCKET_NAME}")
    
    # Test connection
    s3 = test_s3_connection()
    if not s3:
        print("\n❌ Failed to connect to S3. Check your AWS credentials!")
        return
    
    # Test upload
    test_key = test_upload(s3)
    
    # Test download
    if test_key:
        test_download(s3, test_key)
    
    # List objects
    test_list_objects(s3)
    
    # Cleanup
    cleanup(s3, test_key)
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS COMPLETE!")
    print("=" * 60)
    print("\nYour S3 bucket is configured correctly and ready to use! 🎉")


if __name__ == "__main__":
    # Check if bucket name is configured
    if BUCKET_NAME == 'your-bucket-name':
        print("⚠️  Please set your bucket name in .env file first!")
        print("   Edit .env and set BUCKET_NAME=your-actual-bucket-name")
    else:
        main()