import pandas as pd
import xml.etree.ElementTree as ET
import os
import re
import boto3
from io import BytesIO

s3_client = boto3.client('s3')

def parse_element(element, parent_path=""):
    """
    Recursively parse XML element and its children, flattening the structure.
    """
    parsed_data = {}
    path = f"{parent_path}_{element.tag}" if parent_path else element.tag

    if list(element):
        for child in element:
            parsed_data.update(parse_element(child, path))
    else:
        parsed_data[path] = element.text

    for key, value in element.attrib.items():
        parsed_data[f"{path}_@{key}"] = value

    return parsed_data

def rename_columns(df):
    """
    Rename DataFrame columns by removing everything before the last '@' or ':'.
    """
    def extract_column_name(col):
        return re.split('[@:]', col)[-1]

    df.columns = [extract_column_name(col) for col in df.columns]
    return df

def flatten_xml_to_df(xml_content):
    """
    Flatten XML content into a pandas DataFrame, including nested elements and attributes.
    """
    tree = ET.ElementTree(ET.fromstring(xml_content))
    root = tree.getroot()

    rows = []
    for child in root:
        rows.append(parse_element(child))
    
    df = pd.DataFrame(rows)
    df = rename_columns(df)
    
    return df

def lambda_handler(event, context):
    """
    Lambda function handler.
    This function is triggered by an S3 event when a new XML file is uploaded.
    It processes the XML, flattens it, and stores the resulting Parquet file back in S3.
    """
    # Get S3 bucket and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    xml_file_key = event['Records'][0]['s3']['object']['key']
    
    # Download the XML file from S3
    xml_file_obj = s3_client.get_object(Bucket=bucket_name, Key=xml_file_key)
    xml_content = xml_file_obj['Body'].read().decode('utf-8')
    
    # Flatten the XML content to a DataFrame
    df = flatten_xml_to_df(xml_content)
    
    # Save the DataFrame to a Parquet file (in memory)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    # Define the output Parquet file key
    parquet_file_key = xml_file_key.replace('.xml', '.parquet')
    
    # Upload the Parquet file to S3
    s3_client.put_object(Bucket=bucket_name, Key=parquet_file_key, Body=parquet_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': f"Successfully converted {xml_file_key} to {parquet_file_key}"
    }
