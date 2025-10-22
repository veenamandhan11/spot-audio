import json
import os
import subprocess
from datetime import datetime

# Configuration
JSON_FILE_PATH = "./new_creatives.json"
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"
STATION_ID = "153"
TARGET_PATH = r"C:\temp"

# Constants
INI_PATH = r"C:\Program Files\Media Monitors"
GETMEDIA_EXE = r"C:\Program Files\Media Monitors\Getmedia.exe"

def convert_time_format(time_str):
    """
    Convert time from '2025-10-18 09:30:00' format to '20251018-09:30:00' format
    """
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y%m%d-%H:%M:%S")

def create_ini_file(creative):
    """
    Create .ini file for a creative
    """
    creative_id = creative["creative_id"]
    start_time = convert_time_format(creative["start_time"])
    end_time = convert_time_format(creative["end_time"])
    
    ini_content = f"""/u:{USERNAME}
/p:{PASSWORD}
/w:https://data.mediamonitors.com/mmwebservices/
/r:{STATION_ID}
/i:{creative_id}
/s:{start_time}
/e:{end_time}
/t:{TARGET_PATH}\\
/n:{creative_id}
/l"""
    
    ini_filename = f"{creative_id}.ini"
    ini_filepath = os.path.join(INI_PATH, ini_filename)
    
    with open(ini_filepath, 'w') as f:
        f.write(ini_content)
    
    return ini_filename

def run_getmedia(ini_filename):
    """
    Run Getmedia.exe with the specified .ini file
    """
    try:
        # Change to the Media Monitors directory
        os.chdir(INI_PATH)
        
        # Run the command
        cmd = [GETMEDIA_EXE, f"/f:{ini_filename}", "/s"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        print(f"Successfully processed {ini_filename}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error processing {ini_filename}: {e}")
        print(f"Return code: {e.returncode}")
        print(f"Output: {e.output}")
        return False
    except Exception as e:
        print(f"Unexpected error processing {ini_filename}: {e}")
        return False

def main():
    try:
        # Read JSON file
        with open(JSON_FILE_PATH, 'r') as f:
            data = json.load(f)
        
        print(f"Processing {data['count']} creatives...")
        print(f"Test mode: {data['test_mode']}")
        
        # Create target directory if it doesn't exist
        os.makedirs(TARGET_PATH, exist_ok=True)
        
        success_count = 0
        total_count = len(data['creatives'])
        
        for creative in data['creatives']:
            print(f"\nProcessing creative: {creative['creative_id']} - {creative['creative_name']}")
            
            # Create .ini file
            ini_filename = create_ini_file(creative)
            print(f"Created {ini_filename}")
            
            # Run Getmedia.exe
            if run_getmedia(ini_filename):
                success_count += 1
        
        print(f"\n--- Summary ---")
        print(f"Total creatives: {total_count}")
        print(f"Successfully processed: {success_count}")
        print(f"Failed: {total_count - success_count}")
        print(f"Audio files saved to: {TARGET_PATH}")
        
    except FileNotFoundError:
        print(f"Error: JSON file not found at {JSON_FILE_PATH}")
        print("Please check the file path in the configuration section.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {JSON_FILE_PATH}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()