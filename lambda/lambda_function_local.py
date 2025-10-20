"""
Local testing version with SSL workaround
"""

import json
import boto3
import urllib3
from datetime import datetime
import hashlib
import os
import logging
from typing import Dict, List, Any, Optional
import traceback
import ssl
import certifi

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# Initialize HTTP client with proper SSL context for Mac
# Create custom SSL context
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_REQUIRED

# For local testing on Mac, we need to handle SSL differently
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs=certifi.where(),
    ssl_context=ssl_context
)

# Environment variables
BUCKET_NAME = os.environ.get('BUCKET_NAME')
SECRET_NAME = os.environ.get('SECRET_NAME', 'data-pipeline-config')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Main Lambda handler - this is what AWS calls
    """
    
    # Start execution
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 DATA PIPELINE EXECUTION STARTED (Local Mode)")
    logger.info(f"⏰ Start Time: {start_time.isoformat()}")
    logger.info(f"📦 Bucket: {BUCKET_NAME}")
    logger.info("=" * 60)
    
    # Initialize results tracker
    results = {
        'execution_id': context.request_id if context else 'local-test',
        'start_time': start_time.isoformat(),
        'end_time': None,
        'duration_seconds': None,
        'success': True,
        'sources_processed': [],
        'total_records': 0,
        'errors': [],
        'files_created': []
    }
    
    try:
        # Step 1: Get configuration
        logger.info("\n📋 Step 1: Loading configuration...")
        config = get_configuration()
        
        if not config:
            raise Exception("Failed to load configuration")
        
        logger.info(f"   ✅ Configuration loaded")
        logger.info(f"   📊 Data sources to process: {len(config['data_sources'])}")
        
        # Step 2: Process each data source
        logger.info("\n🔄 Step 2: Processing data sources...")
        
        for source_name, source_config in config['data_sources'].items():
            logger.info(f"\n   📊 Processing {source_name.upper()}...")
            
            try:
                # Extract data from API
                raw_data = extract_data_safe(source_name, source_config)
                
                if raw_data:
                    # Transform the data
                    transformed_data = transform_data(source_name, raw_data)
                    
                    # Load to S3
                    file_path = load_to_s3(source_name, transformed_data)
                    
                    # Update results
                    results['sources_processed'].append(source_name)
                    results['total_records'] += len(transformed_data)
                    results['files_created'].append(file_path)
                    
                    logger.info(f"   ✅ {source_name}: {len(transformed_data)} records processed")
                else:
                    logger.warning(f"   ⚠️ {source_name}: No data received")
                    
            except Exception as e:
                error_msg = f"Error processing {source_name}: {str(e)}"
                logger.error(f"   ❌ {error_msg}")
                results['errors'].append(error_msg)
                continue
        
        # Step 3: Generate summary
        logger.info("\n📊 Step 3: Generating summary...")
        summary_path = save_execution_summary(results, config)
        results['files_created'].append(summary_path)
        
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        results['success'] = False
        results['errors'].append(f"Fatal error: {str(e)}")
    
    # Calculate execution time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    results['end_time'] = end_time.isoformat()
    results['duration_seconds'] = duration
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📈 EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"⏱️  Duration: {duration:.2f} seconds")
    logger.info(f"📊 Sources Processed: {len(results['sources_processed'])}/{len(config['data_sources'] if config else {})}")
    logger.info(f"📝 Total Records: {results['total_records']}")
    logger.info(f"📁 Files Created: {len(results['files_created'])}")
    logger.info(f"❌ Errors: {len(results['errors'])}")
    logger.info(f"✅ Status: {'SUCCESS' if results['success'] else 'FAILED'}")
    logger.info("=" * 60)
    
    return {
        'statusCode': 200 if results['success'] else 500,
        'body': json.dumps(results, default=str),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


def get_configuration() -> Optional[Dict]:
    """Get configuration - with fallback for local testing"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        config = json.loads(response['SecretString'])
        return config
    except Exception as e:
        logger.warning(f"Cannot get config from Secrets Manager: {e}")
        logger.info("Using default configuration...")
        
        # Return default configuration
        return {
            'data_sources': {
                'marketing': {
                    'name': 'FakeStore API',
                    'url': 'https://fakestoreapi.com/products',
                    'default_limit': 10
                },
                'sales': {
                    'name': 'JSONPlaceholder',
                    'url': 'https://jsonplaceholder.typicode.com/posts',
                    'default_limit': 10
                },
                'crm': {
                    'name': 'RandomUser API',
                    'url': 'https://randomuser.me/api/',
                    'default_limit': 10
                }
            }
        }


def extract_data_safe(source_name: str, config: Dict) -> Optional[List]:
    """Extract data with better error handling and correct URLs"""
    
    # Try using requests library first (better SSL handling)
    try:
        import requests
        
        # Build correct URLs for each API
        if source_name == 'marketing':
            # FakeStore API - supports ?limit parameter
            url = "https://fakestoreapi.com/products"
            params = {'limit': config.get('default_limit', 10)}
            
        elif source_name == 'sales':
            # JSONPlaceholder - use different endpoint format
            # Gets first N posts directly
            limit = config.get('default_limit', 10)
            url = f"https://jsonplaceholder.typicode.com/posts"
            params = {'_limit': limit}  # JSONPlaceholder uses _limit
            
        elif source_name == 'crm':
            # RandomUser API - uses ?results parameter
            url = "https://randomuser.me/api/"
            params = {'results': config.get('default_limit', 10)}
        else:
            url = config['url']
            params = {}
        
        logger.info(f"      🔗 Fetching from: {url}")
        logger.info(f"      📊 Parameters: {params}")
        
        # Make request with timeout
        response = requests.get(
            url, 
            params=params,
            timeout=30, 
            verify=certifi.where(),
            headers={
                'User-Agent': 'Mozilla/5.0 (compatible; DataPipeline/1.0)',
                'Accept': 'application/json'
            }
        )
        
        logger.info(f"      📡 Response status: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"      ❌ HTTP {response.status_code}")
            logger.error(f"      Response: {response.text[:200]}")
            return None
        
        # Parse JSON
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"      ❌ Invalid JSON response: {e}")
            logger.error(f"      Response text: {response.text[:200]}")
            return None
        
        # Handle different response formats
        if isinstance(data, dict):
            if 'results' in data:  # RandomUser format
                return data['results']
            elif 'data' in data:
                return data['data']
            elif 'products' in data:
                return data['products']
            else:
                # Single object, wrap in list
                return [data]
        elif isinstance(data, list):
            return data[:config.get('default_limit', 10)]  # Ensure we respect limit
        else:
            return [data]
            
    except ImportError:
        logger.warning("requests library not available, using urllib3")
        
        # Fallback to urllib3 with SSL workaround
        try:
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            http_local = urllib3.PoolManager(
                cert_reqs='CERT_NONE',
                ssl_context=ssl_context
            )
            
            # Build URLs with correct parameters
            if source_name == 'marketing':
                url = f"https://fakestoreapi.com/products?limit={config.get('default_limit', 10)}"
            elif source_name == 'sales':
                # Use the correct JSONPlaceholder endpoint
                limit = config.get('default_limit', 10)
                url = f"https://jsonplaceholder.typicode.com/posts?_limit={limit}"
            elif source_name == 'crm':
                url = f"https://randomuser.me/api/?results={config.get('default_limit', 10)}"
            else:
                url = config['url']
            
            logger.info(f"      🔗 Fetching (urllib3): {url}")
            
            response = http_local.request(
                'GET', 
                url, 
                timeout=30,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; DataPipeline/1.0)',
                    'Accept': 'application/json'
                }
            )
            
            logger.info(f"      📡 Response status: {response.status}")
            
            if response.status != 200:
                logger.error(f"      ❌ HTTP {response.status}")
                return None
            
            # Parse JSON
            try:
                data = json.loads(response.data.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"      ❌ Invalid JSON: {e}")
                logger.error(f"      Response: {response.data[:200]}")
                return None
            
            if isinstance(data, dict):
                if 'results' in data:
                    return data['results']
                elif 'data' in data:
                    return data['data']
                else:
                    return [data]
            elif isinstance(data, list):
                return data[:config.get('default_limit', 10)]
            else:
                return [data]
                
        except Exception as e:
            logger.error(f"      ❌ Extract error: {e}")
            return None
            
    except Exception as e:
        logger.error(f"      ❌ Extract error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def transform_data(source_name: str, raw_data: List) -> List[Dict]:
    """Transform raw data into standardized format"""
    transformed = []
    
    for idx, record in enumerate(raw_data):
        unique_string = f"{source_name}_{idx}_{datetime.now().isoformat()}_{json.dumps(record)}"
        record_id = hashlib.md5(unique_string.encode()).hexdigest()[:12]
        
        transformed_record = {
            'record_id': record_id,
            'source': source_name,
            'extracted_at': datetime.now().isoformat(),
            'extracted_date': datetime.now().strftime('%Y-%m-%d'),
            'raw_data': record
        }
        
        if source_name == 'marketing':
            transformed_record['product'] = {
                'id': record.get('id'),
                'title': record.get('title', ''),
                'price': float(record.get('price', 0)),
                'category': record.get('category', ''),
                'description': record.get('description', '')[:200],
                'image': record.get('image', ''),
                'rating': record.get('rating', {})
            }
            
        elif source_name == 'sales':
            transformed_record['sale'] = {
                'id': record.get('id'),
                'user_id': record.get('userId'),
                'title': record.get('title', ''),
                'body': record.get('body', '')[:200]
            }
            
        elif source_name == 'crm':
            if 'name' in record:
                name = record['name']
                transformed_record['customer'] = {
                    'first_name': name.get('first', ''),
                    'last_name': name.get('last', ''),
                    'full_name': f"{name.get('first', '')} {name.get('last', '')}",
                    'email': record.get('email', ''),
                    'phone': record.get('phone', ''),
                    'country': record.get('location', {}).get('country', ''),
                    'city': record.get('location', {}).get('city', ''),
                    'registered_date': record.get('registered', {}).get('date', '')
                }
        
        transformed.append(transformed_record)
    
    return transformed


def load_to_s3(source_name: str, data: List[Dict]) -> str:
    """Load transformed data to S3"""
    current_date = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    s3_key = f"data/{source_name}/date={current_date}/{source_name}_{timestamp}.json"
    
    metadata = {
        'source': source_name,
        'record_count': str(len(data)),
        'extracted_date': current_date,
        'extracted_timestamp': datetime.now().isoformat()
    }
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data, indent=2, default=str),
        ContentType='application/json',
        Metadata=metadata
    )
    
    logger.info(f"      📁 Saved to: s3://{BUCKET_NAME}/{s3_key}")
    
    return s3_key


def save_execution_summary(results: Dict, config: Dict) -> str:
    """Save execution summary to S3"""
    summary = {
        'execution_id': results['execution_id'],
        'execution_date': datetime.now().strftime('%Y-%m-%d'),
        'execution_time': datetime.now().isoformat(),
        'duration_seconds': results['duration_seconds'],
        'success': results['success'],
        'statistics': {
            'sources_configured': len(config.get('data_sources', {})),
            'sources_processed': len(results['sources_processed']),
            'total_records': results['total_records'],
            'files_created': len(results['files_created']),
            'errors': len(results['errors'])
        },
        'sources_processed': results['sources_processed'],
        'files_created': results['files_created'],
        'errors': results['errors']
    }
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    s3_key = f"metadata/executions/date={current_date}/execution_{results['execution_id']}.json"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(summary, indent=2, default=str),
        ContentType='application/json'
    )
    
    logger.info(f"   📁 Summary saved to: s3://{BUCKET_NAME}/{s3_key}")
    
    return s3_key


# For local testing
if __name__ == "__main__":
    import sys
    
    # Install required packages for local testing
    try:
        import requests
    except ImportError:
        print("📦 Installing requests library for better SSL handling...")
        os.system("pip install requests")
        import requests
    
    try:
        import certifi
    except ImportError:
        print("📦 Installing certifi for SSL certificates...")
        os.system("pip install certifi")
        import certifi
    
    if not BUCKET_NAME:
        print("⚠️  Please set BUCKET_NAME environment variable")
        print("   export BUCKET_NAME=your-bucket-name")
        sys.exit(1)
    
    class MockContext:
        request_id = f"local-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        function_name = "data-pipeline-etl"
        function_version = "$LATEST"
        
    print("🧪 Running Lambda function locally with SSL fixes...")
    result = lambda_handler({}, MockContext())
    
    print("\n📊 Execution Results:")
    print(json.dumps(json.loads(result['body']), indent=2))