import json
import os
import subprocess
import time
import concurrent.futures
import threading
import argparse
import shutil
from datetime import datetime
from pathlib import Path

# Configuration
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"

# Paths
INI_BASE_PATH = r"C:\Program Files\Media Monitors"
GETMEDIA_EXE = r"C:\Program Files\Media Monitors\Getmedia.exe"
TEMP_BASE_PATH = r"C:\temp"

# Project paths (relative to script location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)  # Parent of scripts folder
SUMMARIES_FOLDER = os.path.join(PROJECT_ROOT, "summaries")
FAILED_ADS_FOLDER = os.path.join(PROJECT_ROOT, "failed_ads")

# Processing settings
BATCH_SIZE = 10
BATCH_TIMEOUT = 60  # 1 minute per batch
BATCH_START_DELAY = 10  # 10 seconds between batch starts
MAX_RETRIES = 1  # Retry failed creatives once

def ensure_folders():
    """Create necessary folders if they don't exist"""
    os.makedirs(SUMMARIES_FOLDER, exist_ok=True)
    os.makedirs(FAILED_ADS_FOLDER, exist_ok=True)
    print(f"✓ Ensured folders exist: summaries/, failed_ads/")

def extract_datetime_from_filename(json_path):
    """
    Extract datetime range from filename like:
    ads_20251018_000000_20251018_235959.json
    Returns: (start_datetime_str, end_datetime_str, datetime_range_str)
    """
    filename = os.path.basename(json_path)
    # Remove 'ads_' prefix and '.json' suffix
    datetime_part = filename.replace('ads_', '').replace('.json', '')
    # Split into start and end
    parts = datetime_part.split('_')
    if len(parts) == 4:  # Changed from 6 to 4
        start_str = f"{parts[0]}_{parts[1]}"  # YYYYMMDD_HHMMSS
        end_str = f"{parts[2]}_{parts[3]}"    # YYYYMMDD_HHMMSS
        return start_str, end_str, datetime_part
    return None, None, None

def convert_time_format(time_str):
    """
    Convert time from '2025-10-18 04:47:22.000' format to '20251018-04:47:22' format
    """
    if '.' in time_str:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    else:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y%m%d-%H:%M:%S")

def create_ini_file(creative, ini_folder, target_folder):
    """
    Create .ini file for a creative
    """
    aircheck_id = creative["aircheck_id"]
    station_id = creative["station_id"]
    start_time = convert_time_format(creative["start_time"])
    end_time = convert_time_format(creative["end_time"])
    
    ini_content = f"""/u:{USERNAME}
/p:{PASSWORD}
/w:https://data.mediamonitors.com/mmwebservices/
/r:{station_id}
/i:{aircheck_id}
/s:{start_time}
/e:{end_time}
/t:{target_folder}\\
/n:{aircheck_id}
/l"""
    
    ini_filename = f"{aircheck_id}.ini"
    ini_filepath = os.path.join(ini_folder, ini_filename)
    
    with open(ini_filepath, 'w') as f:
        f.write(ini_content)
    
    return ini_filename

def run_single_getmedia(aircheck_id, ini_folder):
    """
    Run Getmedia.exe for a single aircheck_id
    Returns aircheck_id if successful, None if failed
    """
    try:
        ini_filename = f"{aircheck_id}.ini"
        
        # Run the command
        cmd = [GETMEDIA_EXE, f"/f:{ini_filename}", "/s"]
        subprocess.run(cmd, capture_output=True, text=True, cwd=ini_folder)
        
        return aircheck_id
        
    except Exception as e:
        return None

def check_batch_success(batch_aircheck_ids, target_folder):
    """
    Check which aircheck_ids in the batch were successful based on PCM file existence
    Returns list of successful aircheck_ids and list of failed ones
    """
    successful = []
    failed = []
    
    for aircheck_id in batch_aircheck_ids:
        pcm_file = os.path.join(target_folder, f"{aircheck_id}_pcm.wav")
        if os.path.exists(pcm_file):
            successful.append(aircheck_id)
        else:
            failed.append(aircheck_id)
    
    return successful, failed

def update_summary_file(batch_creatives, batch_num, summary_file, target_folder, is_first_batch=False):
    """
    Update summary file with batch results
    """
    mode = 'w' if is_first_batch else 'a'
    
    with open(summary_file, mode) as summary:
        if is_first_batch:
            summary.write("=== MEDIA PROCESSING SUMMARY ===\n")
            summary.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            summary.write("=" * 50 + "\n\n")
        
        summary.write(f"BATCH {batch_num} RESULTS:\n")
        summary.write("-" * 30 + "\n")
        
        for creative in batch_creatives:
            aircheck_id = creative["aircheck_id"]
            creative_id = creative["creative_id"]
            creative_name = creative["creative_name"]
            out_file = os.path.join(target_folder, f"{aircheck_id}.out")
            
            summary.write(f"Aircheck ID: {aircheck_id}\n")
            summary.write(f"Creative ID: {creative_id}\n")
            summary.write(f"Creative Name: {creative_name}\n")
            summary.write(f"Station ID: {creative['station_id']}\n")
            summary.write("-" * 20 + "\n")
            
            if os.path.exists(out_file):
                try:
                    with open(out_file, 'r') as f:
                        summary.write(f.read())
                except Exception as e:
                    summary.write(f"Error reading .out file: {e}")
            else:
                summary.write("No .out file found for this aircheck.")
            
            summary.write("\n" + "-" * 20 + "\n\n")
        
        summary.write("=" * 50 + "\n\n")

def delayed_batch_check_and_log(batch_creatives, batch_num, futures, all_successful, all_failed, 
                                summary_file, target_folder):
    """
    Wait for timeout, then check success and update summary in background
    """
    try:
        concurrent.futures.wait(futures, timeout=BATCH_TIMEOUT)
    except Exception as e:
        print(f"Batch {batch_num} timeout or error: {e}")
    
    # Check which ones were successful
    batch_aircheck_ids = [creative["aircheck_id"] for creative in batch_creatives]
    successful, failed = check_batch_success(batch_aircheck_ids, target_folder)
    
    # Thread-safe updates to shared lists
    all_successful.extend(successful)
    all_failed.extend(failed)
    
    print(f"Batch {batch_num} completed: {len(successful)}/{len(batch_creatives)} successful")
    
    # Update summary file
    is_first = batch_num == 1
    update_summary_file(batch_creatives, batch_num, summary_file, target_folder, is_first)

def process_batch(batch_creatives, batch_num, total_batches, all_successful, all_failed, 
                 ini_folder, summary_file, target_folder):
    """
    Start a batch of creatives in parallel and return immediately
    The success checking and logging happens in a background thread after timeout
    """
    batch_aircheck_ids = [creative["aircheck_id"] for creative in batch_creatives]
    
    print(f"Starting batch {batch_num}/{total_batches} ({len(batch_creatives)} items)...")
    
    # Start all processes in parallel
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_SIZE)
    futures = {executor.submit(run_single_getmedia, aircheck_id, ini_folder): aircheck_id 
              for aircheck_id in batch_aircheck_ids}
    
    # Start background thread to handle timeout checking and logging
    check_thread = threading.Thread(
        target=delayed_batch_check_and_log,
        args=(batch_creatives, batch_num, futures, all_successful, all_failed, 
              summary_file, target_folder)
    )
    check_thread.daemon = True
    check_thread.start()
    
    return check_thread, executor

def copy_pcm_to_desktop(target_folder, desktop_folder_name):
    """
    Copy all *_pcm.wav files from target folder to Desktop folder,
    renaming them from <aircheckId>_pcm.wav to <aircheckId>.wav
    """
    desktop_path = Path.home() / "Desktop"
    destination_folder = desktop_path / desktop_folder_name
    
    try:
        destination_folder.mkdir(exist_ok=True)
        print(f"\n✓ Desktop folder created: {destination_folder}")
        
        # Find all *_pcm.wav files in target folder
        pcm_files = list(Path(target_folder).glob("*_pcm.wav"))
        
        if not pcm_files:
            print(f"⚠ No PCM files found in {target_folder}")
            return 0
        
        print(f"Copying {len(pcm_files)} PCM files to Desktop...")
        
        copied_count = 0
        failed_count = 0
        
        for pcm_file in pcm_files:
            try:
                filename = pcm_file.name
                new_filename = filename.replace('_pcm.wav', '.wav')
                destination_file = destination_folder / new_filename
                
                shutil.copy2(pcm_file, destination_file)
                copied_count += 1
                
            except Exception as e:
                print(f"Failed to copy {filename}: {e}")
                failed_count += 1
        
        print(f"✓ Copied {copied_count} files to Desktop")
        if failed_count > 0:
            print(f"⚠ Failed to copy {failed_count} files")
        
        return copied_count
        
    except Exception as e:
        print(f"Error copying to Desktop: {e}")
        return 0

def cleanup_folders(ini_folder, target_folder):
    """
    Delete INI folder and temp subfolder
    """
    try:
        if os.path.exists(ini_folder):
            shutil.rmtree(ini_folder)
            print(f"✓ Deleted INI folder: {ini_folder}")
        
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)
            print(f"✓ Deleted temp folder: {target_folder}")
            
    except Exception as e:
        print(f"⚠ Error during cleanup: {e}")

def retry_failed_creatives(failed_creatives, ini_folder, target_folder):
    """
    Retry failed creatives one more time
    Returns list of still-failed creatives after retry
    """
    if not failed_creatives:
        return []
    
    print(f"\n=== RETRYING {len(failed_creatives)} FAILED CREATIVES ===")
    
    retry_successful = []
    still_failed = []
    
    for creative in failed_creatives:
        aircheck_id = creative["aircheck_id"]
        print(f"Retrying: {aircheck_id}")
        
        result = run_single_getmedia(aircheck_id, ini_folder)
        time.sleep(2)  # Small delay between retries
        
        # Check if successful
        pcm_file = os.path.join(target_folder, f"{aircheck_id}_pcm.wav")
        if os.path.exists(pcm_file):
            retry_successful.append(aircheck_id)
            print(f"  ✓ Success on retry")
        else:
            still_failed.append(creative)
            print(f"  ✗ Still failed")
    
    print(f"\nRetry results: {len(retry_successful)} succeeded, {len(still_failed)} still failed")
    
    return still_failed

def save_failed_creatives(failed_creatives, datetime_range_str):
    """
    Save failed creatives to JSON file in failed_ads folder
    """
    if not failed_creatives:
        return None
    
    failed_file = os.path.join(FAILED_ADS_FOLDER, f"failed_ads_{datetime_range_str}.json")
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'failed_count': len(failed_creatives),
        'creatives': failed_creatives
    }
    
    with open(failed_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"✓ Failed creatives saved to: {failed_file}")
    return failed_file

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download creative audio files from Media Monitors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Example:
  python get_creatives.py --json "../creatives_metadata/ads_20251018_000000_20251020_235959.json"
        '''
    )
    
    parser.add_argument('--json', required=True, 
                       help='Path to the creatives metadata JSON file')
    
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()
    json_path = args.json
    
    # Ensure folders exist
    ensure_folders()
    
    try:
        # Read JSON file
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Extract datetime info from filename
        start_dt, end_dt, datetime_range_str = extract_datetime_from_filename(json_path)
        
        if not datetime_range_str:
            print("Error: Could not extract datetime from filename")
            return
        
        print(f"\n{'='*70}")
        print(f"CREATIVE AUDIO DOWNLOADER")
        print(f"{'='*70}")
        print(f"Processing {data['count']} creatives")
        print(f"Date range: {data['date_range']['start']} to {data['date_range']['end']}")
        print(f"Batch size: {BATCH_SIZE}, Batch timeout: {BATCH_TIMEOUT}s")
        print(f"Batch start delay: {BATCH_START_DELAY}s")
        print(f"{'='*70}\n")
        
        # Create folders with datetime naming
        ini_folder = os.path.join(INI_BASE_PATH, f"inis_{datetime_range_str}")
        target_folder = os.path.join(TEMP_BASE_PATH, f"ads_{datetime_range_str}")
        desktop_folder_name = f"ads_{datetime_range_str}"
        
        os.makedirs(ini_folder, exist_ok=True)
        os.makedirs(target_folder, exist_ok=True)
        
        print(f"✓ Created INI folder: {ini_folder}")
        print(f"✓ Created temp folder: {target_folder}\n")
        
        # Summary file path
        summary_file = os.path.join(SUMMARIES_FOLDER, f"summary_ads_{datetime_range_str}.txt")
        
        creatives = data['creatives']
        total_count = len(creatives)
        
        # Create all .ini files first
        print("Creating all .ini files...")
        for creative in creatives:
            create_ini_file(creative, ini_folder, target_folder)
        print(f"✓ Created {total_count} .ini files\n")
        
        # Process in batches with overlapping execution
        all_successful = []
        all_failed = []
        active_threads = []
        active_executors = []
        
        # Split into batches
        batches = [creatives[i:i + BATCH_SIZE] for i in range(0, total_count, BATCH_SIZE)]
        total_batches = len(batches)
        
        for batch_num, batch_creatives in enumerate(batches, 1):
            # Start batch processing
            check_thread, executor = process_batch(
                batch_creatives, batch_num, total_batches, all_successful, all_failed,
                ini_folder, summary_file, target_folder
            )
            active_threads.append(check_thread)
            active_executors.append(executor)
            
            # Wait before starting next batch (unless it's the last batch)
            if batch_num < total_batches:
                time.sleep(BATCH_START_DELAY)
        
        # Wait for all background threads to complete
        print(f"\nWaiting for all batches to complete...")
        for thread in active_threads:
            thread.join()
        
        # Clean up executors
        for executor in active_executors:
            executor.shutdown(wait=True)
        
        # Get failed creatives with full details
        failed_creatives = []
        for creative in creatives:
            if creative["aircheck_id"] in all_failed:
                failed_creatives.append(creative)
        
        # Retry failed creatives
        if failed_creatives:
            still_failed = retry_failed_creatives(failed_creatives, ini_folder, target_folder)
            
            # Update successful list with retry successes
            retry_successful_ids = set(c["aircheck_id"] for c in failed_creatives) - set(c["aircheck_id"] for c in still_failed)
            all_successful.extend(list(retry_successful_ids))
            
            # Save still-failed creatives
            if still_failed:
                save_failed_creatives(still_failed, datetime_range_str)
        else:
            still_failed = []
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"DOWNLOAD SUMMARY")
        print(f"{'='*70}")
        print(f"Total creatives: {total_count}")
        print(f"Successfully downloaded: {len(all_successful)}")
        print(f"Failed after retry: {len(still_failed)}")
        print(f"Summary file: {summary_file}")
        
        if still_failed:
            print(f"\nFailed creatives:")
            for failed in still_failed[:5]:  # Show first 5
                print(f"  - {failed['aircheck_id']} | {failed['creative_name']}")
            if len(still_failed) > 5:
                print(f"  ... and {len(still_failed) - 5} more")
        
        # Copy PCM files to Desktop
        print(f"\n{'='*70}")
        print(f"COPYING TO DESKTOP")
        print(f"{'='*70}")
        copied_count = copy_pcm_to_desktop(target_folder, desktop_folder_name)
        
        # Cleanup
        print(f"\n{'='*70}")
        print(f"CLEANUP")
        print(f"{'='*70}")
        cleanup_folders(ini_folder, target_folder)
        
        print(f"\n{'='*70}")
        print(f"✓ PHASE 2 COMPLETED SUCCESSFULLY!")
        print(f"{'='*70}")
        print(f"Audio files location: Desktop/{desktop_folder_name}")
        print(f"Total files copied: {copied_count}")
        
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_path}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_path}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()