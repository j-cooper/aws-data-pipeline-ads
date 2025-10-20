#!/usr/bin/env python3
"""
Deploy Lambda function to AWS
"""

import boto3
import json
import sys
import os
from botocore.exceptions import ClientError

# Configuration
FUNCTION_NAME = 'data-pipeline-etl'
ROLE_NAME = 'lambda-data-pipeline-role'
RUNTIME = 'python3.9'
TIMEOUT = 60
MEMORY_SIZE = 256

# Get bucket name
print("📦 Lambda Deployment Configuration")
print("=" * 60)
BUCKET_NAME = input("Enter your S3 bucket name: ").strip()

# Initialize clients
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')


def get_role_arn():
    """Get IAM role ARN"""
    try:
        response = iam_client.get_role(RoleName=ROLE_NAME)
        return response['Role']['Arn']
    except ClientError:
        print(f"❌ Role {ROLE_NAME} not found. Run create_lambda_role.py first!")
        sys.exit(1)


def deploy_lambda():
    """Deploy or update Lambda function"""
    
    print("\n" + "=" * 60)
    print("🚀 DEPLOYING LAMBDA FUNCTION")
    print("=" * 60)
    
    # Get role ARN
    role_arn = get_role_arn()
    print(f"\n📋 Configuration:")
    print(f"   Function Name: {FUNCTION_NAME}")
    print(f"   Runtime: {RUNTIME}")
    print(f"   Timeout: {TIMEOUT} seconds")
    print(f"   Memory: {MEMORY_SIZE} MB")
    print(f"   Role: {ROLE_NAME}")
    print(f"   Bucket: {BUCKET_NAME}")
    
    # Check if deployment package exists
    if not os.path.exists('lambda_function.zip'):
        print("\n❌ lambda_function.zip not found!")
        print("   Run ./scripts/prepare_lambda.sh first")
        sys.exit(1)
    
    # Read the ZIP file
    with open('lambda_function.zip', 'rb') as f:
        zip_data = f.read()
    
    # Environment variables for Lambda
    # NOTE: AWS_REGION is automatically set by Lambda - don't include it!
    environment = {
        'Variables': {
            'BUCKET_NAME': BUCKET_NAME,
            'SECRET_NAME': 'data-pipeline-config'
        }
    }
    
    try:
        # Try to create the function
        print(f"\n🔄 Creating Lambda function...")
        
        response = lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_data},
            Description='Data pipeline ETL function',
            Timeout=TIMEOUT,
            MemorySize=MEMORY_SIZE,
            Environment=environment,
            Tags={
                'Project': 'DataPipeline',
                'Environment': 'Development',
                'CreatedBy': 'Python'
            }
        )
        
        print(f"   ✅ Lambda function created successfully!")
        print(f"   ARN: {response['FunctionArn']}")
        print(f"   Version: {response['Version']}")
        print(f"   State: {response['State']}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            # Function exists, update it
            print(f"\n🔄 Function already exists, updating...")
            
            try:
                # Update code first
                print("   📦 Updating function code...")
                code_response = lambda_client.update_function_code(
                    FunctionName=FUNCTION_NAME,
                    ZipFile=zip_data
                )
                print(f"      ✅ Code updated (Version: {code_response['Version']})")
                
                # Then update configuration
                print("   ⚙️ Updating function configuration...")
                config_response = lambda_client.update_function_configuration(
                    FunctionName=FUNCTION_NAME,
                    Runtime=RUNTIME,
                    Handler='lambda_function.lambda_handler',
                    Description='Data pipeline ETL function (Updated)',
                    Timeout=TIMEOUT,
                    MemorySize=MEMORY_SIZE,
                    Environment=environment
                )
                
                print(f"      ✅ Configuration updated")
                print(f"   ✅ Lambda function updated successfully!")
                print(f"   ARN: {config_response['FunctionArn']}")
                
                response = config_response
                
            except ClientError as update_error:
                print(f"   ❌ Update failed: {update_error}")
                sys.exit(1)
            
        else:
            print(f"   ❌ Error: {e}")
            sys.exit(1)
    
    print(f"\n✅ Deployment complete!")
    return response['FunctionArn']


def test_lambda():
    """Test the deployed Lambda function"""
    
    print("\n" + "=" * 60)
    print("🧪 TESTING DEPLOYED FUNCTION")
    print("=" * 60)
    
    print(f"\n🔄 Invoking Lambda function...")
    
    try:
        # Invoke the function
        response = lambda_client.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType='RequestResponse',
            LogType='Tail'
        )
        
        # Read response
        payload = json.loads(response['Payload'].read())
        
        print(f"\n📊 Response:")
        print(f"   Status Code: {response['StatusCode']}")
        
        if response['StatusCode'] == 200:
            if 'body' in payload:
                body = json.loads(payload['body'])
                print(f"   Success: {body.get('success', 'Unknown')}")
                print(f"   Records Processed: {body.get('total_records', 0)}")
                
                if body.get('sources_processed'):
                    print(f"   Sources: {', '.join(body['sources_processed'])}")
                
                if body.get('errors'):
                    print(f"   ⚠️ Errors: {len(body['errors'])}")
                    for error in body['errors'][:3]:  # Show first 3 errors
                        print(f"      - {error}")
                
                print(f"\n✅ Lambda function is working!")
            else:
                print(f"   Response: {json.dumps(payload, indent=2)}")
        else:
            print(f"   ❌ Function returned error")
            print(json.dumps(payload, indent=2))
            
    except Exception as e:
        print(f"   ❌ Error testing function: {e}")
        import traceback
        traceback.print_exc()


def check_logs():
    """Show how to check CloudWatch logs"""
    
    print("\n" + "=" * 60)
    print("📋 CLOUDWATCH LOGS")
    print("=" * 60)
    
    log_group = f"/aws/lambda/{FUNCTION_NAME}"
    
    print(f"\n📊 To view logs, run:")
    print(f"   aws logs tail {log_group} --follow")
    
    print(f"\n🔍 Or check recent logs:")
    print(f"   aws logs tail {log_group} --since 5m")
    
    print(f"\n🌐 Or view in AWS Console:")
    print(f"   1. Go to CloudWatch > Log groups")
    print(f"   2. Find: {log_group}")
    print(f"   3. Click on latest log stream")


def main():
    """Main execution"""
    
    print("\n🚀 Lambda Deployment Script")
    print("=" * 60)
    
    # Deploy function
    function_arn = deploy_lambda()
    
    # Ask if user wants to test
    print("\n" + "=" * 60)
    test_now = input("\n🧪 Test the deployed function now? (y/n): ").strip().lower()
    
    if test_now == 'y':
        test_lambda()
    
    # Show log information
    check_logs()
    
    print("\n" + "=" * 60)
    print("🎉 DEPLOYMENT SUCCESSFUL!")
    print("=" * 60)
    print(f"\nYour Lambda function is now live in AWS!")
    print(f"Function Name: {FUNCTION_NAME}")
    print(f"ARN: {function_arn}")
    
    print(f"\n📝 Next steps:")
    print(f"   1. Check CloudWatch Logs for execution details")
    print(f"   2. Verify data in S3:")
    print(f"      aws s3 ls s3://{BUCKET_NAME}/data/ --recursive --human-readable")
    print(f"   3. Set up scheduled execution (EventBridge)")
    print(f"      python scripts/setup_schedule.py")
    
    print(f"\n💡 Quick test command:")
    print(f"   aws lambda invoke --function-name {FUNCTION_NAME} response.json && cat response.json | jq")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Deployment cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)