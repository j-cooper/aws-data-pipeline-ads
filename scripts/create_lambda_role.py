#!/usr/bin/env python3
"""
Create IAM role and policies for Lambda function
"""

import boto3
import json
import time
import sys
from botocore.exceptions import ClientError

# Initialize IAM client
iam = boto3.client('iam')

ROLE_NAME = 'lambda-data-pipeline-role'
BUCKET_NAME = input("Enter your S3 bucket name: ").strip()


def create_lambda_role():
    """Create IAM role for Lambda"""
    
    print("=" * 60)
    print("👤 CREATING IAM ROLE FOR LAMBDA")
    print("=" * 60)
    
    # Trust policy for Lambda
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        # Create role
        print(f"\n📝 Creating role: {ROLE_NAME}")
        
        response = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='IAM role for data pipeline Lambda function',
            Tags=[
                {'Key': 'Project', 'Value': 'DataPipeline'},
                {'Key': 'ManagedBy', 'Value': 'Python'}
            ]
        )
        
        role_arn = response['Role']['Arn']
        print(f"   ✅ Role created: {role_arn}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"   ℹ️ Role already exists, using existing role")
            response = iam.get_role(RoleName=ROLE_NAME)
            role_arn = response['Role']['Arn']
        else:
            print(f"   ❌ Error: {e}")
            sys.exit(1)
    
    return role_arn


def attach_policies():
    """Attach necessary policies to the role"""
    
    print(f"\n📎 Attaching policies...")
    
    # 1. Basic Lambda execution policy
    try:
        iam.attach_role_policy(
            RoleName=ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        print(f"   ✅ Attached: AWSLambdaBasicExecutionRole")
    except ClientError as e:
        if 'already exists' not in str(e):
            print(f"   ⚠️ Warning: {e}")
    
    # 2. Custom policy for S3 and Secrets Manager
    custom_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret"
                ],
                "Resource": "arn:aws:secretsmanager:*:*:secret:data-pipeline-config-*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }
    
    try:
        # Create custom policy
        policy_name = 'lambda-data-pipeline-policy'
        
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(custom_policy)
        )
        
        print(f"   ✅ Created custom policy: {policy_name}")
        
    except ClientError as e:
        print(f"   ❌ Error creating policy: {e}")
        sys.exit(1)
    
    print(f"\n⏳ Waiting for role to propagate...")
    time.sleep(10)
    
    print(f"\n✅ IAM role setup complete!")
    print(f"   Role name: {ROLE_NAME}")
    
    return ROLE_NAME


def main():
    """Main execution"""
    
    print("\n🚀 Setting up IAM role for Lambda")
    
    # Create role
    role_arn = create_lambda_role()
    
    # Attach policies
    attach_policies()
    
    print("\n" + "=" * 60)
    print("✅ IAM ROLE READY")
    print("=" * 60)
    print(f"Role ARN: {role_arn}")
    print(f"Role Name: {ROLE_NAME}")
    
    return role_arn


if __name__ == "__main__":
    main()