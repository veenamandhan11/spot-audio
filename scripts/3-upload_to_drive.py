import os
import argparse
import shutil
from pathlib import Path
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# =============================================================================
# CONFIGURATION
# =============================================================================

# Get script directory and project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Credentials
SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "credentials", "service_account.json")

# Google Drive settings
SCOPES = ['https://www.googleapis.com/auth/drive']
MAIN_FOLDER_NAME = "ads"  # Main folder in Drive root

# Paths
TEMP_BASE_PATH = r"C:\temp"
INI_BASE_PATH = r"C:\Program Files\Media Monitors"

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

def authenticate_drive():
    """Authenticate and return Google Drive service"""
    try:
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"✗ Service account file not found: {SERVICE_ACCOUNT_FILE}")
            return None
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        service = build('drive', 'v3', credentials=credentials)
        print("✓ Authenticated with Google Drive")
        return service
        
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return None

def find_folder_by_name(service, folder_name, parent_id=None):
    """
    Find folder by name in Drive
    Returns folder ID if found, None otherwise
    """
    try:
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=1
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            return files[0]['id']
        return None
        
    except HttpError as e:
        print(f"✗ Error searching for folder: {e}")
        return None

def create_folder(service, folder_name, parent_id=None):
    """
    Create a folder in Google Drive
    Returns folder ID
    """
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        folder = service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        print(f"✓ Created folder: {folder_name}")
        return folder.get('id')
        
    except HttpError as e:
        print(f"✗ Error creating folder: {e}")
        return None

def ensure_folder_structure(service, date_range_folder_name):
    """
    Ensure ads/date-range/ folder structure exists
    Returns the date-range folder ID
    """
    print_section("SETTING UP DRIVE FOLDERS")
    
    # Find or create main "ads" folder
    main_folder_id = find_folder_by_name(service, MAIN_FOLDER_NAME)
    
    if not main_folder_id:
        print(f"Main folder '{MAIN_FOLDER_NAME}' not found, creating it...")
        main_folder_id = create_folder(service, MAIN_FOLDER_NAME)
        if not main_folder_id:
            return None
    else:
        print(f"✓ Found main folder: {MAIN_FOLDER_NAME}")
    
    # Find or create date-range subfolder
    date_folder_id = find_folder_by_name(service, date_range_folder_name, main_folder_id)
    
    if not date_folder_id:
        print(f"Date folder '{date_range_folder_name}' not found, creating it...")
        date_folder_id = create_folder(service, date_range_folder_name, main_folder_id)
        if not date_folder_id:
            return None
    else:
        print(f"✓ Found date folder: {date_range_folder_name}")
    
    return date_folder_id

def file_exists_in_folder(service, filename, folder_id):
    """
    Check if a file with the given name already exists in the folder
    Returns True if exists, False otherwise
    """
    try:
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=1
        ).execute()
        
        files = results.get('files', [])
        return len(files) > 0
        
    except HttpError as e:
        print(f"✗ Error checking file existence: {e}")
        return False

def cleanup_temp_folder(temp_folder):
    """
    Clean temp folder: delete non-PCM files, then rename PCM files
    Returns count of files ready for upload
    """
    print_section("CLEANING TEMP FOLDER")
    
    temp_path = Path(temp_folder)
    
    if not temp_path.exists():
        print(f"✗ Temp folder not found: {temp_folder}")
        return 0
    
    # Step 1: Delete all non-PCM .wav files (compressed versions)
    print("Step 1: Deleting compressed .wav files (non-PCM)...")
    deleted_wav = 0
    for wav_file in temp_path.glob("*.wav"):
        if not wav_file.name.endswith("_pcm.wav"):
            try:
                wav_file.unlink()
                deleted_wav += 1
            except Exception as e:
                print(f"  ⚠ Failed to delete {wav_file.name}: {e}")
    
    print(f"  ✓ Deleted {deleted_wav} compressed .wav files")
    
    # Step 2: Delete .out files
    print("\nStep 2: Deleting .out files...")
    deleted_out = 0
    for out_file in temp_path.glob("*.out"):
        try:
            out_file.unlink()
            deleted_out += 1
        except Exception as e:
            print(f"  ⚠ Failed to delete {out_file.name}: {e}")
    
    print(f"  ✓ Deleted {deleted_out} .out files")
    
    # Step 3: Delete .ini files (if any)
    print("\nStep 3: Deleting .ini files...")
    deleted_ini = 0
    for ini_file in temp_path.glob("*.ini"):
        try:
            ini_file.unlink()
            deleted_ini += 1
        except Exception as e:
            print(f"  ⚠ Failed to delete {ini_file.name}: {e}")
    
    print(f"  ✓ Deleted {deleted_ini} .ini files")
    
    # Step 4: Rename PCM files (remove _pcm suffix)
    print("\nStep 4: Renaming PCM files...")
    renamed_count = 0
    pcm_files = list(temp_path.glob("*_pcm.wav"))
    
    for pcm_file in pcm_files:
        try:
            new_name = pcm_file.name.replace("_pcm.wav", ".wav")
            new_path = pcm_file.parent / new_name
            pcm_file.rename(new_path)
            renamed_count += 1
        except Exception as e:
            print(f"  ⚠ Failed to rename {pcm_file.name}: {e}")
    
    print(f"  ✓ Renamed {renamed_count} PCM files")
    
    print(f"\n✓ Cleanup complete: {renamed_count} files ready for upload")
    
    return renamed_count

def upload_file(service, file_path, folder_id):
    """
    Upload a single file to Google Drive folder
    Returns True if successful, False otherwise
    """
    try:
        filename = os.path.basename(file_path)
        
        # Check if file already exists
        if file_exists_in_folder(service, filename, folder_id):
            print(f"  ⊘ Skipped (already exists): {filename}")
            return True
        
        # Upload file
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(
            file_path,
            mimetype='audio/wav',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        print(f"  ✓ Uploaded: {filename}")
        return True
        
    except HttpError as e:
        print(f"  ✗ Failed to upload {filename}: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Error uploading {filename}: {e}")
        return False

def upload_folder_contents(service, temp_folder, folder_id):
    """
    Upload all .wav files from temp folder to Drive folder
    Returns (success_count, failed_count, skipped_count)
    """
    print_section("UPLOADING FILES TO DRIVE")
    
    wav_files = list(Path(temp_folder).glob("*.wav"))
    
    if not wav_files:
        print(f"⚠ No .wav files found in {temp_folder}")
        return 0, 0, 0
    
    print(f"Found {len(wav_files)} .wav files to upload\n")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, wav_file in enumerate(wav_files, 1):
        print(f"[{i}/{len(wav_files)}] ", end="")
        
        # Check if already exists before uploading
        filename = wav_file.name
        if file_exists_in_folder(service, filename, folder_id):
            print(f"⊘ Skipped (already exists): {filename}")
            skipped_count += 1
            continue
        
        success = upload_file(service, str(wav_file), folder_id)
        
        if success:
            success_count += 1
        else:
            failed_count += 1
    
    return success_count, failed_count, skipped_count

def cleanup_folders(ini_folder, temp_folder):
    """
    Delete INI folder and temp folder after successful upload
    """
    print_section("CLEANUP")
    
    try:
        # Delete INI folder
        if os.path.exists(ini_folder):
            shutil.rmtree(ini_folder)
            print(f"✓ Deleted INI folder: {ini_folder}")
        else:
            print(f"⊘ INI folder not found (already deleted?): {ini_folder}")
        
        # Delete temp folder
        if os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
            print(f"✓ Deleted temp folder: {temp_folder}")
        else:
            print(f"⊘ Temp folder not found (already deleted?): {temp_folder}")
            
    except Exception as e:
        print(f"⚠ Error during cleanup: {e}")

def generate_folder_name(start_datetime, end_datetime):
    """Generate folder name from datetime strings"""
    def format_datetime(dt_str):
        dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        return dt.strftime("%Y%m%d_%H%M%S")
    
    start_formatted = format_datetime(start_datetime)
    end_formatted = format_datetime(end_datetime)
    
    return f"ads_{start_formatted}_{end_formatted}"

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Upload audio files to Google Drive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Example:
  python upload_to_drive.py --start "10/18/2025 00:00:00" --end "10/18/2025 23:59:59"
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
    
    print_header("GOOGLE DRIVE UPLOADER")
    print(f"Start datetime: {start_datetime}")
    print(f"End datetime:   {end_datetime}")
    
    # Generate folder name
    folder_name = generate_folder_name(start_datetime, end_datetime)
    
    # Temp and INI folder paths
    temp_folder = os.path.join(TEMP_BASE_PATH, folder_name)
    ini_folder = os.path.join(INI_BASE_PATH, f"inis_{folder_name.replace('ads_', '')}")
    
    if not os.path.exists(temp_folder):
        print(f"\n✗ Temp folder not found: {temp_folder}")
        print("Make sure Phase 2 completed successfully.")
        return 1
    
    print(f"Temp folder: {temp_folder}")
    print(f"INI folder: {ini_folder}")
    
    # Authenticate
    print_section("AUTHENTICATING")
    service = authenticate_drive()
    
    if not service:
        return 1
    
    # Clean temp folder (delete non-PCM files, rename PCM files)
    files_ready = cleanup_temp_folder(temp_folder)
    
    if files_ready == 0:
        print("\n⚠ No files ready for upload after cleanup")
        return 1
    
    # Ensure folder structure exists in Drive
    date_folder_id = ensure_folder_structure(service, folder_name)
    
    if not date_folder_id:
        print("\n✗ Failed to create/find folder structure in Drive")
        return 1
    
    print(f"\n✓ Drive folder ready: {MAIN_FOLDER_NAME}/{folder_name}")
    
    # Upload files
    success_count, failed_count, skipped_count = upload_folder_contents(
        service, temp_folder, date_folder_id
    )
    
    # Summary
    print_section("UPLOAD SUMMARY")
    total_files = success_count + failed_count + skipped_count
    print(f"Total files found:     {total_files}")
    print(f"Successfully uploaded: {success_count}")
    print(f"Skipped (existing):    {skipped_count}")
    print(f"Failed:                {failed_count}")
    
    # Cleanup folders if upload was successful
    if failed_count == 0 and total_files > 0:
        cleanup_folders(ini_folder, temp_folder)
        print(f"\n{'='*80}")
        print(f"{'✓ PHASE 3 COMPLETED SUCCESSFULLY!'.center(80)}")
        print(f"{'='*80}")
        print(f"\nAll files uploaded to: Google Drive/{MAIN_FOLDER_NAME}/{folder_name}")
        return 0
    elif failed_count > 0:
        print(f"\n⚠ Some files failed to upload")
        print(f"Temp folder preserved for retry: {temp_folder}")
        print(f"INI folder preserved: {ini_folder}")
        return 1
    else:
        print("\n⚠ No files were uploaded")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n✗ Upload interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)