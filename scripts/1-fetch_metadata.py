import requests
import xml.etree.ElementTree as ET
import time
import json
import os
import argparse
from datetime import datetime

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# API Credentials
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"

# File Paths
CREATIVES_FOLDER = "creatives_metadata"
MASTER_CREATIVE_IDS_FILE = "master_creative_ids.json"

# API Settings
RATE_LIMIT_DELAY = 1  # seconds between API calls

# Test Mode Configuration
TEST_MODE = False  # Set to True to test with first station only, False for production

# =============================================================================
# END CONFIGURATION SECTION
# =============================================================================

class MediaMonitorsTracker:
    def __init__(self, username=USERNAME, password=PASSWORD):
        self.username = username
        self.password = password
        self.creatives_folder = CREATIVES_FOLDER
        self.master_ids_file = os.path.join(CREATIVES_FOLDER, MASTER_CREATIVE_IDS_FILE)
        self.test_mode = TEST_MODE
        
        # Ensure creatives folder exists
        self.ensure_creatives_folder()
        
    def ensure_creatives_folder(self):
        """Create creatives metadata folder if it doesn't exist"""
        if not os.path.exists(self.creatives_folder):
            os.makedirs(self.creatives_folder)
            print(f"Created directory: {self.creatives_folder}")

    def generate_filename(self, start_datetime, end_datetime):
        """Generate filename based on datetime range"""
        def format_datetime(dt_str):
            # Parse "10/18/2025 00:00:00" format
            dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
            return dt.strftime("%Y%m%d_%H%M%S")
        
        start_formatted = format_datetime(start_datetime)
        end_formatted = format_datetime(end_datetime)
        
        filename = f"ads_{start_formatted}_{end_formatted}.json"
        return os.path.join(self.creatives_folder, filename)

    def load_master_creative_ids(self):
        """Load master list of creative IDs"""
        if os.path.exists(self.master_ids_file):
            try:
                with open(self.master_ids_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('creative_ids', []))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Error reading master creative IDs file: {e}")
                print("Starting with empty master list")
                return set()
        else:
            print("Master creative IDs file not found, starting with empty list")
            return set()

    def save_master_creative_ids(self, creative_ids_set):
        """Save master list of creative IDs"""
        data = {
            'last_updated': datetime.now().isoformat(),
            'total_count': len(creative_ids_set),
            'creative_ids': sorted(list(creative_ids_set))
        }
        
        with open(self.master_ids_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Updated master creative IDs file with {len(creative_ids_set)} total IDs")

    def filter_new_creatives(self, creatives):
        """Filter out creatives that already exist in master list"""
        existing_ids = self.load_master_creative_ids()
        
        new_creatives = []
        for creative in creatives:
            if creative['creative_id'] not in existing_ids:
                new_creatives.append(creative)
        
        print(f"Filtering: {len(creatives)} total creatives, {len(existing_ids)} already known, {len(new_creatives)} new")
        
        return new_creatives

    def update_master_with_new_creatives(self, new_creatives):
        """Update master list with new creative IDs"""
        existing_ids = self.load_master_creative_ids()
        
        new_ids = {creative['creative_id'] for creative in new_creatives}
        updated_ids = existing_ids.union(new_ids)
        
        self.save_master_creative_ids(updated_ids)
        
        return len(new_ids)

    def get_licensed_stations(self):
        """Get all licensed stations from the API"""
        url = "https://data.mediamonitors.com/mmwebservices/service1.asmx/GetLicensedStations"
        params = {
            'username': self.username,
            'password': self.password
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching stations: {e}")
            return None

    def get_airplay_snapshot(self, station_id, start_time, end_time):
        """Get airplay snapshot for a station - returns count, sequences, and creative data"""
        url = "https://data.mediamonitors.com/mmwebservices/service1.asmx/GetAirPlaySnapshotString"
        params = {
            'stationID': station_id,
            'username': self.username,
            'password': self.password,
            'startTimeStr': start_time,
            'endTimeStr': end_time
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            root = ET.fromstring(response.text)
            
            # Count Table3 elements and extract creative data
            table3_count = 0
            creatives = {}  # Use dict for deduplication by creative_id
            
            for elem in root.iter():
                if elem.tag.endswith('Table3'):
                    table3_count += 1
                    
                    # Extract creative information
                    creative_data = {}
                    for child in elem:
                        tag_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        creative_data[tag_name] = child.text
                    
                    # Add to creatives dict if we have the key fields
                    creative_id = creative_data.get('CreativeID')
                    aircheck_id = creative_data.get('aircheck_id')
                    creative_name = creative_data.get('Account_x002F_Title')
                    start_time_val = creative_data.get('start_time')
                    end_time_val = creative_data.get('end_time')
                    
                    if creative_id and creative_name:
                        creatives[creative_id] = {
                            'creative_id': creative_id,
                            'aircheck_id': aircheck_id,
                            'creative_name': creative_name,
                            'station_id': station_id,
                            'start_time': start_time_val,
                            'end_time': end_time_val
                        }
            
            # Convert dict values back to list
            creatives_list = list(creatives.values())
            
            return table3_count, creatives_list
            
        except requests.RequestException as e:
            print(f"Error fetching snapshot for station {station_id}: {e}")
            return 0, []
        except ET.ParseError as e:
            print(f"Error parsing XML for station {station_id}: {e}")
            return 0, []

    def parse_stations_xml(self, stations_xml):
        """Parse stations XML and return list of station objects"""
        try:
            root = ET.fromstring(stations_xml)
            stations = []
            for elem in root.iter():
                station_id_child = None
                for child in elem:
                    if child.tag.endswith('StationID'):
                        station_id_child = child
                        break
                if station_id_child is not None:
                    stations.append(elem)
            return stations
        except ET.ParseError as e:
            print(f"Error parsing stations XML: {e}")
            return []

    def extract_station_info(self, station_elem):
        """Extract station ID from station element"""
        station_id = None
        for child in station_elem:
            if child.tag.endswith('StationID'):
                station_id = child.text
                break
        return station_id

    def get_airplay_data(self, start_datetime, end_datetime):
        """Get airplay data for configured date range"""
        print(f"Getting airplay data for date range: {start_datetime} to {end_datetime}")
        
        # Get all stations
        stations_xml = self.get_licensed_stations()
        if not stations_xml:
            print("Failed to get stations")
            return []
        
        # Parse stations
        stations = self.parse_stations_xml(stations_xml)
        if not stations:
            print("No stations found")
            return []
        
        print(f"Found {len(stations)} total stations")
        
        # Determine which stations to process
        stations_to_process = [stations[0]] if self.test_mode else stations
        mode_text = "TEST MODE: Processing first station only" if self.test_mode else f"PRODUCTION MODE: Processing all {len(stations)} stations"
        print(f"=== {mode_text} ===")
        
        all_creatives = {}  # Use dict for global deduplication by creative_id
        total_records = 0
        processed_stations = 0
        
        for i, station_elem in enumerate(stations_to_process):
            station_id = self.extract_station_info(station_elem)
            if not station_id:
                print(f"Skipping station {i+1}: No station ID found")
                continue
            
            print(f"Processing station {i+1}/{len(stations_to_process)}: {station_id}")
            
            # Get airplay snapshot for this station
            count, creatives = self.get_airplay_snapshot(station_id, start_datetime, end_datetime)
            
            print(f"  Station {station_id}: {count} records, {len(creatives)} unique creatives")
            
            # Merge creatives into global dict (automatic deduplication by creative_id)
            for creative in creatives:
                creative_id = creative['creative_id']
                all_creatives[creative_id] = creative
            
            total_records += count
            processed_stations += 1
            
            # Rate limiting between stations (except for last station)
            if i < len(stations_to_process) - 1:
                time.sleep(RATE_LIMIT_DELAY)
        
        # Convert dict values back to list
        all_creatives_list = list(all_creatives.values())
        
        print(f"\n=== SUMMARY ===")
        print(f"Processed stations: {processed_stations}")
        print(f"Total airplay records: {total_records}")
        print(f"Unique creatives found: {len(all_creatives_list)}")
        
        return all_creatives_list

    def save_creatives(self, creatives, start_datetime, end_datetime, timestamp=None):
        """Save creatives data to JSON file"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        filename = self.generate_filename(start_datetime, end_datetime)
        
        output_data = {
            'timestamp': timestamp,
            'count': len(creatives),
            'test_mode': self.test_mode,
            'date_range': {
                'start': start_datetime,
                'end': end_datetime
            },
            'creatives': creatives
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        return filename

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Fetch media monitoring metadata for a date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  Single day:
    python fetch_metadata.py --start "10/18/2025 00:00:00" --end "10/18/2025 23:59:59"
  
  Multiple days:
    python fetch_metadata.py --start "10/18/2025 00:00:00" --end "10/20/2025 23:59:59"
  
  Custom time range:
    python fetch_metadata.py --start "10/18/2025 08:00:00" --end "10/18/2025 17:00:00"
        '''
    )
    
    parser.add_argument('--start', required=True, 
                       help='Start datetime in format "MM/DD/YYYY HH:MM:SS"')
    parser.add_argument('--end', required=True,
                       help='End datetime in format "MM/DD/YYYY HH:MM:SS"')
    
    return parser.parse_args()

def validate_datetime_format(dt_str):
    """Validate datetime string format"""
    try:
        datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        return True
    except ValueError:
        return False

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Validate datetime formats
    if not validate_datetime_format(args.start):
        print(f"Error: Invalid start datetime format. Expected 'MM/DD/YYYY HH:MM:SS', got '{args.start}'")
        return
    
    if not validate_datetime_format(args.end):
        print(f"Error: Invalid end datetime format. Expected 'MM/DD/YYYY HH:MM:SS', got '{args.end}'")
        return
    
    # Create tracker instance
    tracker = MediaMonitorsTracker()
    
    print("Media Monitors Metadata Fetcher")
    print("=" * 70)
    print(f"Mode: {'TEST' if TEST_MODE else 'PRODUCTION'}")
    print(f"Username: {USERNAME}")
    print(f"Date Range: {args.start} to {args.end}")
    print(f"Rate Limit Delay: {RATE_LIMIT_DELAY}s")
    print(f"Output Folder: {CREATIVES_FOLDER}")
    print(f"Master IDs File: {tracker.master_ids_file}")
    print("=" * 70)
    
    # Get airplay data for the specified date range
    all_creatives = tracker.get_airplay_data(args.start, args.end)
    
    if all_creatives:
        # Filter out creatives that already exist
        new_creatives = tracker.filter_new_creatives(all_creatives)
        
        if new_creatives:
            # Save new creatives to JSON file
            filename = tracker.save_creatives(new_creatives, args.start, args.end)
            print(f"\n✓ New creatives saved to: {filename}")
            
            # Update master creative IDs list
            added_count = tracker.update_master_with_new_creatives(new_creatives)
            print(f"✓ Added {added_count} new creative IDs to master list")
            
            # Show sample of new creative data
            print(f"\nSample of {len(new_creatives)} new creatives:")
            for i, creative in enumerate(new_creatives[:3]):
                print(f"  {i+1}. Creative ID: {creative['creative_id']}")
                print(f"      Aircheck ID: {creative['aircheck_id']}")
                print(f"      Station: {creative['station_id']}")
                print(f"      Name: {creative['creative_name']}")
                print(f"      Start: {creative['start_time']}")
                print(f"      End: {creative['end_time']}")
            
            if len(new_creatives) > 3:
                print(f"... and {len(new_creatives) - 3} more")
            
            print(f"\n✓ Phase 1 completed successfully!")
            
        else:
            print(f"\n⚠ No new creatives found - all {len(all_creatives)} creatives already exist in master list")
            print("No JSON file created (no new data to save)")
    else:
        print("\n⚠ No creatives found in the specified date range")

if __name__ == "__main__":
    main()