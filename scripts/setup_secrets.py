#!/usr/bin/env python3
"""
Setup AWS Secrets Manager for our data pipeline
This stores our API configurations securely in AWS
"""

import boto3
import json
import sys
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
SECRET_NAME = 'data-pipeline-config'

def create_or_update_secret():
    """Create or update the secret in AWS Secrets Manager"""
    
    print("=" * 60)
    print("🔐 SETTING UP AWS SECRETS MANAGER")
    print("=" * 60)
    
    # Initialize Secrets Manager client
    try:
        client = boto3.client('secretsmanager', region_name=AWS_REGION)
        print(f"\n✅ Connected to AWS Secrets Manager")
        print(f"   Region: {AWS_REGION}")
    except Exception as e:
        print(f"\n❌ Failed to connect to AWS: {e}")
        print("   Please check your AWS credentials")
        return False
    
    # Our configuration data
    secret_data = {
        "version": "1.0.0",
        "created_at": datetime.now().isoformat(),
        "description": "Configuration for data pipeline ETL process",
        
        "data_sources": {
            "marketing": {
                "name": "FakeStore API",
                "description": "Fake e-commerce product data for marketing analysis",
                "url": "https://fakestoreapi.com/products",
                "endpoints": {
                    "all_products": "/products",
                    "single_product": "/products/{id}",
                    "categories": "/products/categories",
                    "category_products": "/products/category/{category}"
                },
                "rate_limit": None,
                "auth_required": False,
                "headers": {
                    "Accept": "application/json"
                },
                "timeout": 30,
                "max_records": 50
            },
            
            "sales": {
                "name": "JSONPlaceholder API",
                "description": "Fake JSON data simulating sales transactions",
                "url": "https://jsonplaceholder.typicode.com",
                "endpoints": {
                    "posts": "/posts",
                    "comments": "/comments",
                    "users": "/users"
                },
                "rate_limit": None,
                "auth_required": False,
                "headers": {
                    "Accept": "application/json"
                },
                "timeout": 30,
                "max_records": 100
            },
            
            "crm": {
                "name": "RandomUser API",
                "description": "Random user generator for CRM customer data",
                "url": "https://randomuser.me/api/",
                "parameters": {
                    "results": 50,
                    "nat": "us,gb,ca,au",
                    "format": "json",
                    "seed": "datapipeline"
                },
                "rate_limit": None,
                "auth_required": False,
                "headers": {
                    "Accept": "application/json"
                },
                "timeout": 30,
                "max_records": 50
            }
        },
        
        "processing_config": {
            "batch_size": 100,
            "parallel_processing": False,
            "error_handling": "continue_on_error",
            "retry_config": {
                "max_retries": 3,
                "retry_delay": 5,
                "backoff_multiplier": 2
            },
            "data_quality_checks": {
                "remove_duplicates": True,
                "validate_schema": True,
                "check_null_values": True
            }
        },
        
        "output_config": {
            "format": "json",
            "compression": None,
            "partition_by": ["source", "date"],
            "file_naming": "{source}_{timestamp}.json"
        },
        
        "monitoring": {
            "log_level": "INFO",
            "metrics_enabled": True,
            "alert_on_failure": False,
            "notification_email": None
        },
        
        "tags": {
            "environment": "development",
            "project": "data-pipeline",
            "owner": "data-team",
            "cost-center": "learning"
        }
    }
    
    # Convert to JSON string
    secret_string = json.dumps(secret_data, indent=2)
    
    print(f"\n📋 Secret Configuration:")
    print(f"   Name: {SECRET_NAME}")
    print(f"   Size: {len(secret_string)} bytes")
    print(f"   Data sources: {len(secret_data['data_sources'])}")
    
    try:
        # Try to create the secret
        print(f"\n🔄 Creating secret '{SECRET_NAME}'...")
        
        response = client.create_secret(
            Name=SECRET_NAME,
            Description='Configuration for data pipeline - API endpoints and settings',
            SecretString=secret_string,
            Tags=[
                {'Key': 'Project', 'Value': 'DataPipeline'},
                {'Key': 'Environment', 'Value': 'Development'},
                {'Key': 'ManagedBy', 'Value': 'Python'},
            ]
        )
        
        print(f"\n✅ Secret created successfully!")
        print(f"   ARN: {response['ARN']}")
        print(f"   Version: {response['VersionId']}")
        
    except client.exceptions.ResourceExistsException:
        # Secret already exists, update it
        print(f"\n🔄 Secret already exists, updating...")
        
        response = client.update_secret(
            SecretId=SECRET_NAME,
            Description='Configuration for data pipeline - API endpoints and settings (Updated)',
            SecretString=secret_string
        )
        
        print(f"\n✅ Secret updated successfully!")
        print(f"   ARN: {response['ARN']}")
        print(f"   Version: {response['VersionId']}")
        
    except Exception as e:
        print(f"\n❌ Error managing secret: {e}")
        return False
    
    # Display summary
    print(f"\n" + "=" * 60)
    print("📊 SECRET SUMMARY")
    print("=" * 60)
    print(f"\n🔗 Data Sources Configured:")
    for source_key, source_data in secret_data['data_sources'].items():
        print(f"\n   {source_key.upper()}:")
        print(f"   • Name: {source_data['name']}")
        print(f"   • URL: {source_data['url']}")
        print(f"   • Auth Required: {source_data['auth_required']}")
        print(f"   • Max Records: {source_data['max_records']}")
    
    print(f"\n💰 Estimated Monthly Cost:")
    print(f"   • Secret storage: $0.40")
    print(f"   • API calls: ~$0.00 (minimal usage)")
    print(f"   • Total: ~$0.40/month")
    
    print(f"\n✅ Secrets Manager setup complete!")
    
    return True


def test_secret_retrieval():
    """Test that we can retrieve the secret"""
    
    print(f"\n" + "=" * 60)
    print("🧪 TESTING SECRET RETRIEVAL")
    print("=" * 60)
    
    try:
        client = boto3.client('secretsmanager', region_name=AWS_REGION)
        
        print(f"\n📥 Retrieving secret '{SECRET_NAME}'...")
        
        response = client.get_secret_value(SecretId=SECRET_NAME)
        
        # Parse the secret
        secret_data = json.loads(response['SecretString'])
        
        print(f"\n✅ Secret retrieved successfully!")
        print(f"   Version: {response['VersionId']}")
        print(f"   Last Modified: {response['CreatedDate']}")
        print(f"   Data Sources Found: {list(secret_data['data_sources'].keys())}")
        
        return True
        
    except client.exceptions.ResourceNotFoundException:
        print(f"\n❌ Secret '{SECRET_NAME}' not found!")
        print(f"   Please run the setup first.")
        return False
        
    except Exception as e:
        print(f"\n❌ Error retrieving secret: {e}")
        return False


def main():
    """Main function"""
    
    print("\n🚀 AWS Secrets Manager Setup Tool")
    print("=" * 60)
    
    # Create or update secret
    if create_or_update_secret():
        # Test retrieval
        test_secret_retrieval()
        
        print(f"\n" + "=" * 60)
        print("✅ SETUP COMPLETE!")
        print("=" * 60)
        print(f"\n📝 Next Steps:")
        print(f"   1. Secret is now stored in AWS")
        print(f"   2. Lambda function can access it")
        print(f"   3. No hardcoded credentials needed!")
    else:
        print(f"\n❌ Setup failed. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()