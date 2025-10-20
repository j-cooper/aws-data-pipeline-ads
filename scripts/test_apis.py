#!/usr/bin/env python3
"""
Test all APIs individually to debug issues
"""

import requests
import json
import os
from datetime import datetime

# Set your bucket name
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'your-bucket-name')

def test_marketing_api():
    """Test FakeStore API"""
    print("\n" + "="*60)
    print("🛍️ Testing Marketing API (FakeStore)")
    print("="*60)
    
    url = "https://fakestoreapi.com/products"
    params = {'limit': 5}
    
    try:
        print(f"URL: {url}")
        print(f"Params: {params}")
        
        response = requests.get(url, params=params, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success! Got {len(data)} products")
            if data:
                print(f"Sample: {data[0].get('title', 'No title')[:50]}")
            return True
        else:
            print(f"❌ Failed with status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_sales_api():
    """Test JSONPlaceholder API"""
    print("\n" + "="*60)
    print("📊 Testing Sales API (JSONPlaceholder)")
    print("="*60)
    
    # Try different endpoint formats
    urls_to_try = [
        ("https://jsonplaceholder.typicode.com/posts", {'_limit': 5}),
        ("https://jsonplaceholder.typicode.com/posts", {}),  # Get all, slice later
    ]
    
    for url, params in urls_to_try:
        try:
            print(f"\nTrying: {url}")
            print(f"Params: {params}")
            
            response = requests.get(url, params=params, timeout=10)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # If no limit param worked, slice the results
                if not params and isinstance(data, list):
                    data = data[:5]
                
                print(f"✅ Success! Got {len(data)} posts")
                if data:
                    print(f"Sample: {data[0].get('title', 'No title')[:50]}")
                return True
                
        except Exception as e:
            print(f"❌ Error with {url}: {e}")
            continue
    
    print("❌ All attempts failed")
    return False


def test_crm_api():
    """Test RandomUser API"""
    print("\n" + "="*60)
    print("👥 Testing CRM API (RandomUser)")
    print("="*60)
    
    url = "https://randomuser.me/api/"
    params = {'results': 5}
    
    try:
        print(f"URL: {url}")
        print(f"Params: {params}")
        
        response = requests.get(url, params=params, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            print(f"✅ Success! Got {len(results)} users")
            if results:
                user = results[0]
                name = user.get('name', {})
                print(f"Sample: {name.get('first', '')} {name.get('last', '')}")
            return True
        else:
            print(f"❌ Failed with status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_all_with_boto3():
    """Test with actual Lambda function logic"""
    print("\n" + "="*60)
    print("🧪 Testing Complete Pipeline")
    print("="*60)
    
    import boto3
    
    # Test S3 access
    try:
        s3 = boto3.client('s3')
        s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=1)
        print("✅ S3 access working")
    except Exception as e:
        print(f"❌ S3 access failed: {e}")
        print("   Make sure BUCKET_NAME is set correctly")
        return
    
    # Process each API
    apis = {
        'marketing': {
            'url': 'https://fakestoreapi.com/products',
            'params': {'limit': 3}
        },
        'sales': {
            'url': 'https://jsonplaceholder.typicode.com/posts',
            'params': {}  # Will slice results
        },
        'crm': {
            'url': 'https://randomuser.me/api/',
            'params': {'results': 3}
        }
    }
    
    for name, config in apis.items():
        print(f"\n📊 Processing {name}...")
        
        try:
            response = requests.get(config['url'], params=config['params'], timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Handle different formats
                if name == 'crm':
                    records = data.get('results', [])
                elif name == 'sales' and not config['params']:
                    records = data[:3] if isinstance(data, list) else [data]
                else:
                    records = data if isinstance(data, list) else [data]
                
                # Save to S3
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                key = f"test/{name}_{timestamp}.json"
                
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    Body=json.dumps(records, indent=2),
                    ContentType='application/json'
                )
                
                print(f"   ✅ Saved {len(records)} records to s3://{BUCKET_NAME}/{key}")
                
        except Exception as e:
            print(f"   ❌ Failed: {e}")


def main():
    """Run all tests"""
    print("🚀 API Testing Suite")
    print("="*60)
    
    # Check environment
    if BUCKET_NAME == 'your-bucket-name':
        print("⚠️  Please set BUCKET_NAME environment variable:")
        print("   export BUCKET_NAME=your-actual-bucket-name")
        BUCKET_NAME = input("Enter your bucket name: ").strip()
        os.environ['BUCKET_NAME'] = BUCKET_NAME
    
    print(f"📦 Using bucket: {BUCKET_NAME}")
    
    # Test each API
    results = {
        'marketing': test_marketing_api(),
        'sales': test_sales_api(),
        'crm': test_crm_api()
    }
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    for api, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{api.upper()}: {status}")
    
    if all(results.values()):
        print("\n✅ All APIs working!")
        
        # Test full pipeline
        proceed = input("\nTest full pipeline with S3? (y/n): ")
        if proceed.lower() == 'y':
            test_all_with_boto3()
    else:
        print("\n❌ Some APIs failed. Check the errors above.")
        print("\n💡 Tip: The Lambda will still work with partial data!")


if __name__ == "__main__":
    main()