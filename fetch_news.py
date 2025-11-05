import requests
import boto3
import json
from datetime import datetime
# for our environment variable
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.environ.get("API_KEY")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
S3_BUCKET_RAW = os.environ.get("S3_BUCKET_RAW")
AWS_REGION = os.environ.get("AWS_REGION")

def fetch_news_to_s3():
    # Setup S3 client
    s3 = boto3.client(
        's3', 
        aws_access_key_id=AWS_ACCESS_KEY, 
        aws_secret_access_key=AWS_SECRET_KEY, 
        region_name=AWS_REGION
    )

    # Fetch news from apinews.org
    url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        articles = response.json().get('articles', [])
        
        # Create timestamp for organization
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M")
        filename = f"news_{timestamp}.json"
        
        # Upload to S3 with date partitioning
        s3_key = f"daily/{today}/{filename}"
        
        s3.put_object(
            Bucket=S3_BUCKET_RAW,
            Key=s3_key,
            Body=json.dumps(articles, indent=2)
        )
        
        print(f"✅ Success! Uploaded {len(articles)} articles to s3://{S3_BUCKET_RAW}/{s3_key}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    fetch_news_to_s3()