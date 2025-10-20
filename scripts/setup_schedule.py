#!/usr/bin/env python3
"""
Set up EventBridge rule to trigger Lambda daily
"""

import boto3
import json
from botocore.exceptions import ClientError

# Configuration
FUNCTION_NAME = 'data-pipeline-etl'
RULE_NAME = 'data-pipeline-daily-trigger'
SCHEDULE_EXPRESSION = 'cron(0 2 * * ? *)'  # Daily at 2 AM UTC

# Initialize clients
events_client = boto3.client('events')
lambda_client = boto3.client('lambda')

# Get account ID
sts = boto3.client('sts')
account_id = sts.get_caller_identity()['Account']


def create_schedule():
    """Create EventBridge schedule rule"""
    
    print("=" * 60)
    print("⏰ SETTING UP SCHEDULED EXECUTION")
    print("=" * 60)
    
    print(f"\n📋 Configuration:")
    print(f"   Rule Name: {RULE_NAME}")
    print(f"   Schedule: Daily at 2 AM UTC")
    print(f"   Target: {FUNCTION_NAME}")
    
    # Create or update rule
    try:
        print(f"\n🔄 Creating schedule rule...")
        
        response = events_client.put_rule(
            Name=RULE_NAME,
            ScheduleExpression=SCHEDULE_EXPRESSION,
            State='ENABLED',
            Description='Trigger data pipeline ETL daily at 2 AM UTC'
        )
        
        rule_arn = response['RuleArn']
        print(f"   ✅ Rule created: {RULE_NAME}")
        
    except ClientError as e:
        print(f"   ❌ Error creating rule: {e}")
        return False
    
    # Add Lambda permission
    try:
        print(f"\n🔐 Adding Lambda permission...")
        
        lambda_client.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId=f'{RULE_NAME}-permission',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=rule_arn
        )
        
        print(f"   ✅ Permission added")
        
    except ClientError as e:
        if 'ResourceConflictException' not in str(e):
            print(f"   ⚠️ Warning: {e}")
    
    # Add Lambda as target
    try:
        print(f"\n🎯 Adding Lambda as target...")
        
        lambda_arn = f"arn:aws:lambda:us-east-1:{account_id}:function:{FUNCTION_NAME}"
        
        response = events_client.put_targets(
            Rule=RULE_NAME,
            Targets=[
                {
                    'Id': '1',
                    'Arn': lambda_arn
                }
            ]
        )
        
        if response['FailedEntryCount'] == 0:
            print(f"   ✅ Target added successfully")
        else:
            print(f"   ❌ Failed to add target")
            return False
            
    except ClientError as e:
        print(f"   ❌ Error adding target: {e}")
        return False
    
    print(f"\n✅ Schedule setup complete!")
    print(f"   Your Lambda will run daily at 2 AM UTC")
    print(f"   To test now: aws lambda invoke --function-name {FUNCTION_NAME} response.json")
    
    return True


def main():
    """Main execution"""
    
    print("\n🚀 EventBridge Schedule Setup")
    
    success = create_schedule()
    
    if success:
        print("\n" + "=" * 60)
        print("🎉 SCHEDULE CREATED SUCCESSFULLY!")
        print("=" * 60)
    else:
        print("\n❌ Schedule setup failed")


if __name__ == "__main__":
    main()