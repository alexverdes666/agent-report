"""
Data processing utilities for the agent report scraper.
"""

import pandas as pd
import json
from typing import List, Dict, Any


class DataProcessor:
    """Handles processing and transformation of scraped data."""
    
    def __init__(self):
        pass
    
    def flatten_data(self, scraped_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Flatten complex scraped data into a pandas DataFrame.
        
        Args:
            scraped_data: List of dictionaries containing scraped data
            
        Returns:
            pandas.DataFrame: Flattened data suitable for CSV export
        """
        flattened_records = []
        
        for record in scraped_data:
            # Basic page information
            base_record = {
                'timestamp': record.get('timestamp'),
                'url': record.get('url'),
                'title': record.get('title'),
                'text_length': len(record.get('text_content', '')),
                'num_links': len(record.get('links', [])),
                'num_tables': len(record.get('tables', []))
            }
            
            # If there are tables, create separate records for each table
            tables = record.get('tables', [])
            if tables:
                for table_idx, table in enumerate(tables):
                    table_record = base_record.copy()
                    table_record.update({
                        'table_index': table_idx,
                        'table_rows': len(table.get('rows', [])),
                        'table_data': json.dumps(table.get('rows', []))
                    })
                    flattened_records.append(table_record)
            else:
                flattened_records.append(base_record)
        
        return pd.DataFrame(flattened_records)
    
    def extract_table_data(self, scraped_data: List[Dict[str, Any]]) -> List[pd.DataFrame]:
        """
        Extract and convert table data to separate DataFrames.
        
        Args:
            scraped_data: List of dictionaries containing scraped data
            
        Returns:
            List[pd.DataFrame]: List of DataFrames, one for each table found
        """
        tables = []
        
        for record in scraped_data:
            for table in record.get('tables', []):
                rows = table.get('rows', [])
                if rows:
                    # Use first row as headers if it looks like headers
                    if len(rows) > 1:
                        df = pd.DataFrame(rows[1:], columns=rows[0])
                    else:
                        df = pd.DataFrame(rows)
                    tables.append(df)
        
        return tables
    
    def extract_links(self, scraped_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Extract all links found during scraping.
        
        Args:
            scraped_data: List of dictionaries containing scraped data
            
        Returns:
            pd.DataFrame: DataFrame containing all links with metadata
        """
        all_links = []
        
        for record in scraped_data:
            page_url = record.get('url')
            timestamp = record.get('timestamp')
            
            for link in record.get('links', []):
                all_links.append({
                    'source_page': page_url,
                    'timestamp': timestamp,
                    'link_text': link.get('text'),
                    'link_url': link.get('href')
                })
        
        return pd.DataFrame(all_links)
    
    def generate_summary(self, scraped_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a summary of the scraped data.
        
        Args:
            scraped_data: List of dictionaries containing scraped data
            
        Returns:
            Dict: Summary statistics and information
        """
        if not scraped_data:
            return {"error": "No data to summarize"}
        
        total_pages = len(scraped_data)
        total_links = sum(len(record.get('links', [])) for record in scraped_data)
        total_tables = sum(len(record.get('tables', [])) for record in scraped_data)
        
        # Get unique domains from links
        all_link_urls = []
        for record in scraped_data:
            for link in record.get('links', []):
                all_link_urls.append(link.get('href', ''))
        
        unique_domains = set()
        for url in all_link_urls:
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc
                if domain:
                    unique_domains.add(domain)
            except:
                pass
        
        summary = {
            'total_pages_scraped': total_pages,
            'total_links_found': total_links,
            'total_tables_found': total_tables,
            'unique_domains_linked': len(unique_domains),
            'pages_scraped': [record.get('url') for record in scraped_data],
            'scraping_timespan': {
                'start': min(record.get('timestamp') for record in scraped_data),
                'end': max(record.get('timestamp') for record in scraped_data)
            }
        }
        
        return summary 