import os
import sys
import pickle
from pathlib import Path
from datetime import datetime
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import io

# =============================================================================
# CONFIGURATION
# =============================================================================

# Get script directory and project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Credentials
CREDENTIALS_FOLDER = os.path.join(PROJECT_ROOT, "credentials")
CLIENT_SECRET_FILE = os.path.join(CREDENTIALS_FOLDER, "client_secret.json")
TOKEN_FILE = os.path.join(CREDENTIALS_FOLDER, "token_mac.pickle")  # Different from Windows

# Google Drive settings
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
MAIN_FOLDER_NAME = "ads"  # Main folder in Drive root

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
    """Authenticate using OAuth 2.0 and return Google Drive service"""
    try:
        creds = None
        
        # Check if token.pickle exists (saved credentials)
        if os.path.exists(TOKEN_FILE):
            print("Loading saved credentials...")
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        
        # If no valid credentials, let user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                if not os.path.exists(CLIENT_SECRET_FILE):
                    print(f"✗ OAuth client secret file not found: {CLIENT_SECRET_FILE}")
                    print("\nPlease copy the client_secret.json from Windows to Mac:")
                    print(f"  {CLIENT_SECRET_FILE}")
                    return None
                
                print("Opening browser for authentication...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next time
            print("Saving credentials...")
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build service
        print("Building Drive service...")
        service = build('drive', 'v3', credentials=creds)
        
        # Test connection
        print("Testing connection...")
        service.files().list(pageSize=1).execute()
        
        print("✓ Authenticated with Google Drive")
        return service
        
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def find_main_folder(service):
    """Find the main 'ads' folder in Drive root"""
    try:
        query = f"name='{MAIN_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=10
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            print(f"✓ Found main folder: {MAIN_FOLDER_NAME}")
            return files[0]['id']
        else:
            print(f"✗ Main folder '{MAIN_FOLDER_NAME}' not found in Drive")
            return None
        
    except HttpError as e:
        print(f"✗ Error searching for main folder: {e}")
        return None

def list_subfolders(service, parent_folder_id):
    """List all subfolders in the main ads folder"""
    try:
        query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, createdTime)',
            orderBy='createdTime desc',
            pageSize=100
        ).execute()
        
        folders = results.get('files', [])
        return folders
        
    except HttpError as e:
        print(f"✗ Error listing subfolders: {e}")
        return []

def display_folder_menu(folders):
    """Display folders and get user selection"""
    if not folders:
        print("No folders found in the ads directory.")
        return []
    
    print("Available folders:")
    print("-" * 80)
    
    for i, folder in enumerate(folders, 1):
        # Parse creation time for display
        created_time = folder.get('createdTime', 'Unknown')
        if created_time != 'Unknown':
            dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            created_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"{i:3d}. {folder['name']:<50} | Created: {created_time}")
    
    print("-" * 80)
    print("\nOptions:")
    print("  • Single folder:    enter number (e.g., '3')")
    print("  • Multiple folders: comma-separated (e.g., '1,3,5')")
    print("  • Range:            hyphen (e.g., '1-5')")
    print("  • All folders:      'all'")
    print("  • Latest N folders: 'latest:N' (e.g., 'latest:3')")
    print("  • Cancel:           'q' or Ctrl+C")
    
    while True:
        try:
            selection = input("\nYour choice: ").strip().lower()
            
            if selection in ['q', 'quit', 'exit']:
                return []
            
            if selection == 'all':
                return folders
            
            # Handle latest:N selections
            if selection.startswith('latest:'):
                try:
                    n = int(selection.split(':')[1])
                    return folders[:n]
                except (ValueError, IndexError):
                    print("Invalid format. Use 'latest:N' where N is a number.")
                    continue
            
            selected_folders = []
            
            # Handle comma-separated selections
            if ',' in selection:
                indices = [int(x.strip()) for x in selection.split(',')]
                for idx in indices:
                    if 1 <= idx <= len(folders):
                        selected_folders.append(folders[idx - 1])
                    else:
                        print(f"Invalid selection: {idx}")
                        raise ValueError
            
            # Handle range selections
            elif '-' in selection:
                start, end = map(int, selection.split('-'))
                if 1 <= start <= end <= len(folders):
                    selected_folders = folders[start-1:end]
                else:
                    print(f"Invalid range: {start}-{end}")
                    raise ValueError
            
            # Handle single selection
            else:
                idx = int(selection)
                if 1 <= idx <= len(folders):
                    selected_folders = [folders[idx - 1]]
                else:
                    print(f"Invalid selection: {idx}")
                    raise ValueError
            
            return selected_folders
            
        except (ValueError, IndexError):
            print("Invalid input. Please try again.")
        except KeyboardInterrupt:
            print("\n\nCancelled by user.")
            return []

def list_files_in_folder(service, folder_id):
    """List all .wav files in a folder"""
    try:
        query = f"'{folder_id}' in parents and (mimeType='audio/wav' or name contains '.wav') and trashed=false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, size)',
            pageSize=1000
        ).execute()
        
        files = results.get('files', [])
        return files
        
    except HttpError as e:
        print(f"✗ Error listing files in folder: {e}")
        return []

def format_size(size_bytes):
    """Format file size in human readable format"""
    try:
        size = int(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    except:
        return "Unknown"

def download_file(service, file_id, file_name, local_path):
    """Download a single file from Google Drive"""
    try:
        # Check if file already exists
        if local_path.exists():
            return 'skipped'
        
        # Create directory if it doesn't exist
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download file
        request = service.files().get_media(fileId=file_id)
        
        with open(local_path, 'wb') as local_file:
            downloader = MediaIoBaseDownload(local_file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        
        return 'success'
        
    except HttpError as e:
        print(f"  ✗ Failed to download {file_name}: {e}")
        return 'failed'
    except Exception as e:
        print(f"  ✗ Error downloading {file_name}: {e}")
        return 'failed'

def download_folder(service, folder, local_ads_dir):
    """Download all files from a Drive folder to local directory"""
    folder_name = folder['name']
    folder_id = folder['id']
    
    print(f"\n📁 Downloading folder: {folder_name}")
    print("-" * 80)
    
    # Create local folder path
    local_folder_path = local_ads_dir / folder_name
    
    # List files in Drive folder
    files = list_files_in_folder(service, folder_id)
    
    if not files:
        print(f"  ⚠ No .wav files found in {folder_name}")
        return 0, 0, 0
    
    # Calculate total size
    total_size = sum(int(f.get('size', 0)) for f in files)
    print(f"  Found {len(files)} files (Total: {format_size(total_size)})")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, file_info in enumerate(files, 1):
        file_name = file_info['name']
        file_id = file_info['id']
        file_size = format_size(file_info.get('size', 0))
        local_file_path = local_folder_path / file_name
        
        result = download_file(service, file_id, file_name, local_file_path)
        
        if result == 'success':
            print(f"  [{i}/{len(files)}] ✓ {file_name} ({file_size})")
            success_count += 1
        elif result == 'skipped':
            print(f"  [{i}/{len(files)}] ⊘ {file_name} (already exists)")
            skipped_count += 1
        else:
            failed_count += 1
    
    return success_count, failed_count, skipped_count

def main():
    print_header("GOOGLE DRIVE DOWNLOADER - Mac")
    
    # Setup local ads directory
    local_ads_dir = Path(PROJECT_ROOT) / "ads"
    print(f"Local download directory: {local_ads_dir}")
    
    # Authenticate
    print_section("AUTHENTICATING")
    service = authenticate_drive()
    
    if not service:
        return 1
    
    # Find main ads folder
    print_section("FINDING FOLDERS")
    main_folder_id = find_main_folder(service)
    
    if not main_folder_id:
        return 1
    
    # List all subfolders
    folders = list_subfolders(service, main_folder_id)
    
    if not folders:
        print("No subfolders found in the ads directory.")
        return 1
    
    # Display menu and get user selection
    print_section("FOLDER SELECTION")
    selected_folders = display_folder_menu(folders)
    
    if not selected_folders:
        print("No folders selected. Exiting.")
        return 0
    
    # Download selected folders
    print_section(f"DOWNLOADING {len(selected_folders)} FOLDER(S)")
    total_success = 0
    total_failed = 0
    total_skipped = 0
    
    for folder in selected_folders:
        success, failed, skipped = download_folder(service, folder, local_ads_dir)
        total_success += success
        total_failed += failed
        total_skipped += skipped
    
    # Summary
    print_section("DOWNLOAD SUMMARY")
    total_files = total_success + total_failed + total_skipped
    print(f"Folders processed:         {len(selected_folders)}")
    print(f"Total files:               {total_files}")
    print(f"Successfully downloaded:   {total_success}")
    print(f"Skipped (already exists):  {total_skipped}")
    print(f"Failed:                    {total_failed}")
    print(f"\nDownload location: {local_ads_dir}")
    
    if total_failed == 0:
        print("\n✓ Download completed successfully!")
        return 0
    else:
        print(f"\n⚠ {total_failed} files failed to download")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)