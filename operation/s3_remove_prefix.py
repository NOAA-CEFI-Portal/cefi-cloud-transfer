#!/usr/bin/env python3
"""
S3 Bulk Delete Script with Prefix Support

This script deletes all objects in an S3 bucket that match specified prefixes.
Features:
- Dry-run mode to preview deletions
- Batch deletion for efficiency
- Comprehensive logging
- Support for multiple prefixes
- Error handling and recovery

Usage:
    python s3_bulk_delete.py --dry-run          # Preview deletions
    python s3_bulk_delete.py --delete           # Actually delete
    python s3_bulk_delete.py --prefix "test/"   # Delete specific prefix only
"""

import boto3
import logging
import argparse
import sys
import os
from datetime import datetime
from typing import List, Dict, Tuple
from botocore.exceptions import ClientError, NoCredentialsError

# Configuration
S3_BUCKET_NAME = 'noaa-oar-cefi-regional-mom6-pds'

# List of prefixes to delete
PREFIXES_TO_DELETE = [
    "northeast_pacific/full_domain/hindcast/monthly/raw/r20250509",
    "northeast_pacific/full_domain/hindcast/daily/raw/r20250509",
    "northeast_pacific/full_domain/hindcast/monthly/regrid/r20250509",
    "northeast_pacific/full_domain/hindcast/daily/regrid/r20250509"
]

def setup_logging(log_file: str = None) -> None:
    """Setup logging configuration"""

    if log_file is None:
        log_file = "s3_remove_prefix.log"

    # Remove existing log file if it exists
    if os.path.exists(log_file):
        os.remove(log_file)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return

def get_s3_client():
    """Create and return S3 client with error handling"""
    try:
        session = boto3.Session()
        s3_client = session.client("s3")
        
        return s3_client
        
    except NoCredentialsError:
        logging.error("AWS credentials not found. Please run 'aws configure'")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}")
        sys.exit(1)

def scan_prefix(
        s3_client,
        bucket_name: str,
        prefix: str
    ) -> Tuple[List[Dict], int, int]:
    """
    Scan S3 bucket for objects matching prefix
    
    Returns:
        Tuple of (objects_list, total_count, total_size_bytes)
    """

    try:
        # boto3 pagination mechanism for handling large S3 bucket listings efficiently.
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        objects = []
        total_count = 0
        total_size = 0

        init_scan_info = f"Scanning prefix: {prefix}"
        logging.info(init_scan_info)

        for _, page in enumerate(pages):
            if 'Contents' in page:
                page_objects = page['Contents']
                objects.extend(page_objects)

                page_count = len(page_objects)
                page_size = sum(obj['Size'] for obj in page_objects)

                total_count += page_count
                total_size += page_size

        size_count_info = f"Prefix '{prefix}' scan complete: {total_count} objects, {total_size / (1024**2):.2f} MB total"
        logging.info(size_count_info)

        return objects, total_count, total_size

    except ClientError as e:
        scan_error = f"Error scanning prefix '{prefix}': {e}"
        logging.error(scan_error)
        return [], 0, 0

def delete_objects_batch(s3_client, bucket_name: str, objects_to_delete: List[Dict]) -> Tuple[int, int]:
    """
    Delete a batch of objects (max 1000)
    
    Returns:
        Tuple of (successful_deletions, failed_deletions)
    """

    if not objects_to_delete:
        return 0, 0

    try:
        # Prepare delete request
        # returned object structure of `objects_to_delete`
        # [
        #     {
        #         'Key': 'northeast_pacific/full_domain/hindcast/monthly/raw/r20250509/file1.nc',
        #         'Size': 1234567,
        #         'LastModified': datetime(2025, 5, 9, 12, 0, 0),
        #         'ETag': '"abc123..."',
        #         'StorageClass': 'STANDARD'
        #     },
        #     {
        #         'Key': 'northeast_pacific/full_domain/hindcast/monthly/raw/r20250509/file2.nc', 
        #         'Size': 2345678,
        #         'LastModified': datetime(2025, 5, 9, 12, 5, 0),
        #         'ETag': '"def456..."',
        #         'StorageClass': 'STANDARD'
        #     }
        # ]
        delete_request = {
            'Objects': [{'Key': obj['Key']} for obj in objects_to_delete],
            'Quiet': False
        }

        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete=delete_request
        )
        
        successful = len(response.get('Deleted', []))
        failed = len(response.get('Errors', []))
        
        # Log any errors
        for error in response.get('Errors', []):
            logging.error(f"Failed to delete {error['Key']}: {error['Code']} - {error['Message']}")
        
        if successful > 0:
            logging.info(f"Successfully deleted {successful} objects")
        if failed > 0:
            logging.warning(f"Failed to delete {failed} objects")
            
        return successful, failed
        
    except Exception as e:
        logging.error(f"Error in batch delete: {e}")
        return 0, len(objects_to_delete)

def delete_prefix(s3_client, bucket_name: str, prefix: str, dry_run: bool = True) -> Dict:
    """
    Delete all objects with specified prefix
    
    Returns:
        Dictionary with deletion statistics
    """

    logging.info(f"{'[DRY RUN] ' if dry_run else ''}Processing prefix: {prefix}")

    # Scan for objects
    objects, total_count, total_size = scan_prefix(s3_client, bucket_name, prefix)

    if total_count == 0:
        logging.info(f"No objects found with prefix '{prefix}'")
        return {
            'prefix': prefix,
            'found': 0,
            'deleted': 0,
            'failed': 0,
            'size_mb': 0,
            'dry_run': dry_run
        }

    # Show sample objects
    logging.info(f"Sample objects to {'be deleted' if dry_run else 'delete'}:")
    for i, obj in enumerate(objects[:5]):
        size_mb = obj['Size'] / (1024**2)
        logging.info(f"  {i+1}. {obj['Key']} ({size_mb:.2f} MB)")

    if len(objects) > 5:
        logging.info(f"  ... and {len(objects) - 5} more objects")

    # Summary
    size_mb = total_size / (1024**2)
    logging.info(f"Total: {total_count} objects, {size_mb:.2f} MB")

    if dry_run:
        logging.info(f"[DRY RUN] Would delete {total_count} objects ({size_mb:.2f} MB)")
        return {
            'prefix': prefix,
            'found': total_count,
            'deleted': 0,
            'failed': 0,
            'size_mb': size_mb,
            'dry_run': True
        }

    # Actual deletion
    logging.info(f"Starting deletion of {total_count} objects...")

    total_deleted = 0
    total_failed = 0
    batch_size = 1000  # AWS limit

    # Process in batches
    for i in range(0, len(objects), batch_size):
        batch = objects[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(objects) + batch_size - 1) // batch_size
        
        logging.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} objects)")
        
        deleted, failed = delete_objects_batch(s3_client, bucket_name, batch)
        total_deleted += deleted
        total_failed += failed

    logging.info(f"Deletion complete for prefix '{prefix}': {total_deleted} deleted, {total_failed} failed")

    return {
        'prefix': prefix,
        'found': total_count,
        'deleted': total_deleted,
        'failed': total_failed,
        'size_mb': size_mb,
        'dry_run': False
    }

def bulk_delete_prefixes(prefixes: List[str], bucket_name: str, dry_run: bool = True) -> List[Dict]:
    """
    Delete objects for multiple prefixes
    
    Returns:
        List of deletion statistics for each prefix
    """
    
    s3_client = get_s3_client()
    results = []
    
    logging.info(f"{'[DRY RUN] ' if dry_run else ''}Starting bulk deletion for {len(prefixes)} prefixes")
    logging.info(f"Target bucket: {bucket_name}")
    logging.info(f"Prefixes: {prefixes}")
    
    for i, prefix in enumerate(prefixes):
        logging.info(f"\n{'='*60}")
        logging.info(f"Processing prefix {i+1}/{len(prefixes)}: {prefix}")
        logging.info(f"{'='*60}")
        
        result = delete_prefix(s3_client, bucket_name, prefix, dry_run)
        results.append(result)
    
    # Summary
    logging.info(f"\n{'='*60}")
    logging.info("SUMMARY")
    logging.info(f"{'='*60}")
    
    total_found = sum(r['found'] for r in results)
    total_deleted = sum(r['deleted'] for r in results)
    total_failed = sum(r['failed'] for r in results)
    total_size = sum(r['size_mb'] for r in results)
    
    for result in results:
        status = "DRY RUN" if result['dry_run'] else f"{result['deleted']} deleted, {result['failed']} failed"
        logging.info(f"{result['prefix']:<30} {result['found']:>6} objects ({result['size_mb']:>8.2f} MB) - {status}")
    
    logging.info(f"{'='*60}")
    logging.info(f"TOTALS: {total_found} objects found, {total_size:.2f} MB")
    if not dry_run:
        logging.info(f"        {total_deleted} deleted, {total_failed} failed")
    
    s3_client.close()
    return results

def main():
    """Main function with command line argument parsing"""
    
    parser = argparse.ArgumentParser(description='Bulk delete S3 objects by prefix')
    
    # Action group - mutually exclusive
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--dry-run', action='store_true', 
                            help='Preview what would be deleted without actually deleting')
    action_group.add_argument('--delete', action='store_true',
                            help='Actually delete the objects')
    
    # Optional arguments
    parser.add_argument('--prefix', type=str, 
                       help='Delete only this specific prefix (overrides default list)')
    parser.add_argument('--bucket', type=str, default=S3_BUCKET_NAME,
                       help=f'S3 bucket name (default: {S3_BUCKET_NAME})')
    parser.add_argument('--log-file', type=str,
                       help='Log file path (default: auto-generated with timestamp)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_file)
    
    # Determine prefixes to process
    if args.prefix:
        prefixes = [args.prefix]
        logging.info(f"Processing single prefix: {args.prefix}")
    else:
        prefixes = PREFIXES_TO_DELETE
        logging.info(f"Processing {len(prefixes)} default prefixes")
    
    # Validate prefixes
    if not prefixes:
        logging.error("No prefixes specified for deletion")
        sys.exit(1)
    
    # Confirmation for actual deletion
    if args.delete:
        print("\n" + "="*60)
        print("WARNING: This will permanently delete S3 objects!")
        print(f"Bucket: {args.bucket}")
        print(f"Prefixes: {prefixes}")
        print("="*60)
        
        response = input("Are you sure you want to proceed? Type 'DELETE' to confirm: ")
        if response != 'DELETE':
            print("Deletion cancelled")
            logging.info("Deletion cancelled by user")
            sys.exit(0)
    
    # Perform the operation
    dry_run = args.dry_run
    results = bulk_delete_prefixes(prefixes, args.bucket, dry_run)
    
    # Exit status
    if not dry_run:
        total_failed = sum(r['failed'] for r in results)
        if total_failed > 0:
            logging.warning(f"Some deletions failed. Check log for details.")
            sys.exit(1)
    
    logging.info("Operation completed successfully")

if __name__ == '__main__':
    main()