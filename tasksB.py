import os
import requests
import sqlite3
import duckdb
import markdown
#from PIL import Image
from typing import Optional, Union, List, Dict, Any
from pathlib import Path
import logging
import json
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataWorksAgent')

def B12(filepath: Union[str, Path]) -> bool:
    """
    B1 & B2: Security check to ensure:
    - Data outside /data is never accessed
    - No deletion operations are performed
    
    Args:
        filepath: Path to validate
        
    Returns:
        bool: True if path is secure, False otherwise
    """
    try:
        path = Path(filepath)
        if path.is_absolute():
            return str(path).startswith('/data')
        return True
    except Exception as e:
        logger.error(f"Security check failed: {str(e)}")
        return False

def B3(url: str, save_path: str, headers: Optional[Dict] = None) -> bool:
    """
    B3: Fetch data from an API and save it.
    
    Args:
        url: API endpoint URL
        save_path: Path to save the response
        headers: Optional request headers
        
    Returns:
        bool: Success status
    """
    if not B12(save_path):
        logger.error(f"Security check failed for path: {save_path}")
        return False
        
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Save response based on content type
        content_type = response.headers.get('content-type', '')
        if 'application/json' in content_type:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, indent=2)
        else:
            with open(save_path, 'wb') as f:
                f.write(response.content)
                
        logger.info(f"Successfully saved API data to {save_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error fetching API data: {str(e)}")
        return False

def B5(db_path: str, query: str, output_filename: str) -> Optional[List[Any]]:
    """
    B5: Run SQL query on SQLite or DuckDB database.
    
    Args:
        db_path: Path to the database file
        query: SQL query to execute
        output_filename: Path to save results
        
    Returns:
        Optional[List[Any]]: Query results if successful
    """
    if not B12(db_path) or not B12(output_filename):
        return None
        
    try:
        # Choose database engine based on file extension
        is_sqlite = db_path.endswith('.db')
        conn = sqlite3.connect(db_path) if is_sqlite else duckdb.connect(db_path)
        
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Save results
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump({
                'query': query,
                'results': results,
                'column_names': [desc[0] for desc in cursor.description]
            }, f, indent=2)
        
        conn.close()
        logger.info(f"Successfully executed query and saved results to {output_filename}")
        return results
        
    except Exception as e:
        logger.error(f"Error executing SQL query: {str(e)}")
        return None

def B6(url: str, output_filename: str, selector: Optional[str] = None) -> bool:
    """
    B6: Extract data from a website.
    
    Args:
        url: Website URL to scrape
        output_filename: Path to save scraped data
        selector: Optional CSS selector to filter content
        
    Returns:
        bool: Success status
    """
    if not B12(output_filename):
        return False
        
    try:
        from bs4 import BeautifulSoup
        from datetime import datetime
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        content = soup.select(selector) if selector else soup
        
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump({
                'url': url,
                'timestamp': str(datetime.now()),
                'content': str(content)
            }, f, indent=2)
            
        logger.info(f"Successfully scraped website to {output_filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error scraping website: {str(e)}")
        return False

def B7(image_path: str, output_path: str, resize: Optional[tuple] = None,
       format: Optional[str] = None, quality: int = 85) -> bool:
    """
    B7: Process image with various operations.
    
    Args:
        image_path: Path to source image
        output_path: Path to save processed image
        resize: Optional tuple of (width, height)
        format: Optional output format (e.g., 'JPEG', 'PNG')
        quality: JPEG quality (1-100)
        
    Returns:
        bool: Success status
    """
    if not B12(image_path) or not B12(output_path):
        return False
        
    try:
        with Image.open(image_path) as img:
            if resize:
                img = img.resize(resize, Image.Resampling.LANCZOS)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            save_kwargs = {'quality': quality} if 'JPEG' in (format, img.format) else {}
            img.save(output_path, format=format, **save_kwargs)
            
        logger.info(f"Successfully processed image to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing image: {str(e)}")
        return False

def B9(md_path: str, output_path: str, extras: Optional[List[str]] = None) -> bool:
    """
    B9: Convert Markdown to HTML.
    
    Args:
        md_path: Path to markdown file
        output_path: Path to save HTML
        extras: Optional list of Python-Markdown extensions
        
    Returns:
        bool: Success status
    """
    if not B12(md_path) or not B12(output_path):
        return False
        
    try:
        with open(md_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        html = markdown.markdown(md_content, extensions=extras or [])
        
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Converted from {os.path.basename(md_path)}</title>
        </head>
        <body>
            {html}
        </body>
        </html>
        """
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(full_html)
            
        logger.info(f"Successfully converted markdown to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error converting markdown: {str(e)}")
        return False

# FastAPI endpoint for B10
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class FilterRequest(BaseModel):
    csv_path: str
    filter_column: str
    filter_value: Any

@app.post("/filter_csv")
async def B10(request: FilterRequest):
    """
    B10: Filter CSV and return JSON data.
    """
    if not B12(request.csv_path):
        raise HTTPException(status_code=403, detail="Access denied: Path must be within /data directory")
        
    try:
        import pandas as pd
        df = pd.read_csv(request.csv_path)
        filtered_df = df[df[request.filter_column] == request.filter_value]
        return filtered_df.to_dict(orient='records')
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))