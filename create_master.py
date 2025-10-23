import json
import os
from datetime import datetime

# Configuration
CREATIVES_FOLDER = "creatives_metadata"
MASTER_CREATIVE_IDS_FILE = "master_creative_ids.json"

def create_master_from_existing_files():
    """Create master creative IDs file from all existing JSON files"""
    
    # Ensure folder exists
    if not os.path.exists(CREATIVES_FOLDER):
        print(f"Folder {CREATIVES_FOLDER} doesn't exist!")
        return
    
    master_path = os.path.join(CREATIVES_FOLDER, MASTER_CREATIVE_IDS_FILE)
    all_creative_ids = set()
    processed_files = 0
    
    # Scan all JSON files in the folder
    for filename in os.listdir(CREATIVES_FOLDER):
        if filename.endswith('.json') and filename != MASTER_CREATIVE_IDS_FILE:
            filepath = os.path.join(CREATIVES_FOLDER, filename)
            
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    
                # Extract creative IDs from this file
                creatives = data.get('creatives', [])
                file_creative_ids = {creative['creative_id'] for creative in creatives if 'creative_id' in creative}
                
                all_creative_ids.update(file_creative_ids)
                processed_files += 1
                
                print(f"Processed {filename}: {len(file_creative_ids)} creative IDs")
                
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    # Create master file
    master_data = {
        'last_updated': datetime.now().isoformat(),
        'total_count': len(all_creative_ids),
        'source_files_processed': processed_files,
        'creative_ids': sorted(list(all_creative_ids))
    }
    
    with open(master_path, 'w') as f:
        json.dump(master_data, f, indent=2)
    
    print(f"\n✓ Master file created: {master_path}")
    print(f"✓ Total unique creative IDs: {len(all_creative_ids)}")
    print(f"✓ Files processed: {processed_files}")

if __name__ == "__main__":
    print("Creating master creative IDs file from existing JSONs...")
    print("=" * 50)
    create_master_from_existing_files()