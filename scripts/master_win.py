import subprocess
import sys
import os
import argparse
import json
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Get script directory and project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Script paths
FETCH_METADATA_SCRIPT = os.path.join(SCRIPT_DIR, "fetch_metadata.py")
GET_CREATIVES_SCRIPT = os.path.join(SCRIPT_DIR, "get_creatives.py")

# Folder paths
CREATIVES_METADATA_FOLDER = os.path.join(PROJECT_ROOT, "creatives_metadata")

# =============================================================================
# END CONFIGURATION
# =============================================================================

def print_header(title):
    """Print a formatted header"""
    print(f"\n{'='*80}")
    print(f"{title.center(80)}")
    print(f"{'='*80}\n")

def print_section(title):
    """Print a section separator"""
    print(f"\n{'-'*80}")
    print(f"  {title}")
    print(f"{'-'*80}\n")

def generate_json_filename(start_datetime, end_datetime):
    """Generate expected JSON filename from datetime strings"""
    def format_datetime(dt_str):
        # Parse "10/18/2025 00:00:00" format
        dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        return dt.strftime("%Y%m%d_%H%M%S")
    
    start_formatted = format_datetime(start_datetime)
    end_formatted = format_datetime(end_datetime)
    
    filename = f"ads_{start_formatted}_{end_formatted}.json"
    return os.path.join(CREATIVES_METADATA_FOLDER, filename)

def run_script_with_live_output(script_path, args):
    """
    Run a Python script with live output streaming
    Returns (exit_code, success)
    """
    try:
        cmd = [sys.executable, script_path] + args
        
        # Run with live output (stdout/stderr inherited from parent)
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            text=True
        )
        
        return result.returncode, result.returncode == 0
        
    except Exception as e:
        print(f"✗ Error running script: {e}")
        return 1, False

def check_json_exists(json_path):
    """Check if JSON file exists and return its data"""
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            return True, data
        except Exception as e:
            print(f"Warning: Could not read JSON file: {e}")
            return False, None
    return False, None

def ask_user_confirmation(message):
    """Ask user for yes/no confirmation"""
    while True:
        response = input(f"\n{message} (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' or 'n'")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Master pipeline: Fetch metadata and download creatives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  Single day:
    python master.py --start "10/18/2025 00:00:00" --end "10/18/2025 23:59:59"
  
  Multiple days:
    python master.py --start "10/18/2025 00:00:00" --end "10/20/2025 23:59:59"
  
  Custom time range:
    python master.py --start "10/18/2025 08:00:00" --end "10/18/2025 17:00:00"
        '''
    )
    
    parser.add_argument('--start', required=True, 
                       help='Start datetime in format "MM/DD/YYYY HH:MM:SS"')
    parser.add_argument('--end', required=True,
                       help='End datetime in format "MM/DD/YYYY HH:MM:SS"')
    
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()
    start_datetime = args.start
    end_datetime = args.end
    
    # Track overall execution
    pipeline_start_time = datetime.now()
    
    print_header("MEDIA MONITORS MASTER PIPELINE")
    print(f"Start datetime: {start_datetime}")
    print(f"End datetime:   {end_datetime}")
    print(f"Started at:     {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Expected JSON filename
    expected_json = generate_json_filename(start_datetime, end_datetime)
    
    # =========================================================================
    # PHASE 1: FETCH METADATA
    # =========================================================================
    print_section("PHASE 1: FETCHING METADATA")
    
    fetch_args = ["--start", start_datetime, "--end", end_datetime]
    exit_code, success = run_script_with_live_output(FETCH_METADATA_SCRIPT, fetch_args)
    
    if not success:
        print(f"\n✗ Phase 1 failed with exit code {exit_code}")
        print("Pipeline aborted.")
        return 1
    
    # Check if new JSON was created
    json_exists, json_data = check_json_exists(expected_json)
    
    if not json_exists:
        print_section("NO NEW CREATIVES FOUND")
        print("No new creatives were discovered in this date range.")
        
        # Ask if user wants to re-download existing creatives
        redownload = ask_user_confirmation(
            "Do you want to re-download existing creatives for this date range?"
        )
        
        if not redownload:
            print("\n✓ Pipeline completed (no downloads)")
            return 0
        
        # If user wants to redownload, check if any JSON exists for this range
        # (we'll use the expected filename even if it's old)
        if not os.path.exists(expected_json):
            print(f"\n✗ No metadata file found at: {expected_json}")
            print("Cannot proceed with downloads. Please check your date range.")
            return 1
        
        print("\n→ Proceeding with re-download of existing creatives...")
    else:
        print(f"\n✓ Phase 1 completed: {json_data['count']} new creatives found")
    
    # =========================================================================
    # PHASE 2: DOWNLOAD CREATIVES
    # =========================================================================
    print_section("PHASE 2: DOWNLOADING CREATIVES")
    
    get_creatives_args = ["--json", expected_json]
    exit_code, success = run_script_with_live_output(GET_CREATIVES_SCRIPT, get_creatives_args)
    
    if not success:
        print(f"\n✗ Phase 2 failed with exit code {exit_code}")
        print("Pipeline completed with errors.")
        return 1
    
    print(f"\n✓ Phase 2 completed successfully")
    
    # =========================================================================
    # FINAL SUMMARY
    # =========================================================================
    pipeline_end_time = datetime.now()
    duration = pipeline_end_time - pipeline_start_time
    
    print_header("PIPELINE SUMMARY")
    print(f"Date range:        {start_datetime} to {end_datetime}")
    print(f"Started at:        {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Completed at:      {pipeline_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration:    {duration}")
    print(f"\nMetadata file:     {expected_json}")
    
    if json_exists and json_data:
        print(f"Creatives found:   {json_data['count']}")
    
    print(f"\n{'='*80}")
    print(f"{'✓ PIPELINE COMPLETED SUCCESSFULLY!'.center(80)}")
    print(f"{'='*80}\n")
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n✗ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)