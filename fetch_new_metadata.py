import requests
import xml.etree.ElementTree as ET
import time
import json
import os
import subprocess
import sys
from datetime import datetime

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# API Credentials
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"

# File Paths
SEQUENCE_FILE = "last_sequence.json"
CREATIVES_FOLDER = "creatives_metadata"
MASTER_CREATIVE_IDS_FILE = "master_creative_ids.json"

# Baseline Configuration
BASELINE_START_DATE = "10/20/2025 00:00:00"
BASELINE_END_DATE = "10/20/2025 23:59:59"

# API Settings
RATE_LIMIT_DELAY = 1  # seconds between API calls

# Test Mode Configuration
TEST_MODE = False  # Set to True to test with first station only, False for production

# Next Script Configuration
NEXT_SCRIPT = "get_creatives.py"  # Script to run after completion

# Output Settings
SAVE_DETAILED_RECORDS = True  # Set to False to only save creative summaries

# =============================================================================
# END CONFIGURATION SECTION
# =============================================================================

class MediaMonitorsTracker:
    def __init__(self, username=USERNAME, password=PASSWORD):
        self.username = username
        self.password = password
        self.sequence_file = SEQUENCE_FILE
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

    def generate_filename(self, start_date, end_date):
        """Generate filename based on date range"""
        def parse_date_string(date_str):
            # Parse "10/18/2025 00:00:00" format
            date_part, time_part = date_str.split(' ')
            month, day, year = date_part.split('/')
            hour, minute, second = time_part.split(':')
            return f"{year}{month.zfill(2)}{day.zfill(2)}_{hour.zfill(2)}{minute.zfill(2)}{second.zfill(2)}"
        
        start_formatted = parse_date_string(start_date)
        end_formatted = parse_date_string(end_date)
        
        filename = f"creatives_{start_formatted}_{end_formatted}.json"
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
        # Load existing creative IDs
        existing_ids = self.load_master_creative_ids()
        
        # Filter out existing creatives
        new_creatives = []
        for creative in creatives:
            if creative['creative_id'] not in existing_ids:
                new_creatives.append(creative)
        
        print(f"Filtering: {len(creatives)} total creatives, {len(existing_ids)} already known, {len(new_creatives)} new")
        
        return new_creatives

    def update_master_with_new_creatives(self, new_creatives):
        """Update master list with new creative IDs"""
        # Load existing IDs
        existing_ids = self.load_master_creative_ids()
        
        # Add new IDs
        new_ids = {creative['creative_id'] for creative in new_creatives}
        updated_ids = existing_ids.union(new_ids)
        
        # Save updated list
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
                    
                    # Add to creatives dict if we have the key fields (deduplicates automatically by creative_id)
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
            
            # Get sequence numbers (kept for future use)
            sequences = {}
            for elem in root.iter():
                if elem.tag.endswith('BiggestSequenceForAirPlayChange'):
                    sequences['airplay'] = int(elem.text)
                elif elem.tag.endswith('BiggestSequenceForTitleAssignmentChange'):
                    sequences['title'] = int(elem.text)
                elif elem.tag.endswith('BiggestSequenceForMetaTitleIDChange'):
                    sequences['meta_title'] = int(elem.text)
            
            # Convert dict values back to list
            creatives_list = list(creatives.values())
            
            return table3_count, sequences, creatives_list
            
        except requests.RequestException as e:
            print(f"Error fetching snapshot for station {station_id}: {e}")
            return 0, {}, []
        except ET.ParseError as e:
            print(f"Error parsing XML for station {station_id}: {e}")
            return 0, {}, []

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

    def get_airplay_data(self, start_date=BASELINE_START_DATE, end_date=BASELINE_END_DATE):
        """Get airplay data for configured date range"""
        print(f"Getting airplay data for date range: {start_date} to {end_date}")
        
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
            count, sequences, creatives = self.get_airplay_snapshot(station_id, start_date, end_date)
            
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

    def save_creatives(self, creatives, start_date, end_date, timestamp=None):
        """Save creatives data to JSON file with date-based filename"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        # Generate filename based on date range
        filename = self.generate_filename(start_date, end_date)
        
        output_data = {
            'timestamp': timestamp,
            'count': len(creatives),
            'test_mode': self.test_mode,
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'creatives': creatives
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        return filename

    def run_next_script(self, script_name=NEXT_SCRIPT):
        """Run the next script in the pipeline"""
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        
        # Check if the next script exists
        if not os.path.exists(script_path):
            print(f"Warning: Next script '{script_name}' not found in current directory")
            return False
        
        print(f"\n=== TRIGGERING NEXT SCRIPT ===")
        print(f"Running: {script_name}")
        
        try:
            # Run the next script using the same Python interpreter
            result = subprocess.run([sys.executable, script_path], 
                                  capture_output=True, 
                                  text=True, 
                                  cwd=os.path.dirname(__file__))
            
            print(f"Next script exit code: {result.returncode}")
            
            if result.stdout:
                print("Next script output:")
                print(result.stdout)
            
            if result.stderr:
                print("Next script errors:")
                print(result.stderr)
            
            if result.returncode == 0:
                print(f"✓ Successfully completed {script_name}")
                return True
            else:
                print(f"✗ {script_name} failed with exit code {result.returncode}")
                return False
                
        except Exception as e:
            print(f"Error running next script: {e}")
            return False

    # =========================================================================
    # FUTURE SEQUENCE TRACKING METHODS (KEPT FOR FUTURE USE)
    # =========================================================================
    
    def save_sequence(self, sequence_data):
        """Save sequence data to file (for future use)"""
        with open(self.sequence_file, 'w') as f:
            json.dump(sequence_data, f, indent=2)

    def load_sequence(self):
        """Load sequence data from file (for future use)"""
        if os.path.exists(self.sequence_file):
            with open(self.sequence_file, 'r') as f:
                return json.load(f)
        return None

    def establish_baseline(self, start_date=BASELINE_START_DATE, end_date=BASELINE_END_DATE):
        """Establish baseline by getting snapshot of first station (for future use)"""
        print("Establishing baseline for future sequence tracking...")
        
        # Get all stations
        stations_xml = self.get_licensed_stations()
        if not stations_xml:
            print("Failed to get stations")
            return False
        
        # Parse stations
        stations = self.parse_stations_xml(stations_xml)
        if not stations:
            print("No stations found")
            return False
        
        # Get baseline from first station to capture initial sequence
        first_station = stations[0]
        station_id = self.extract_station_info(first_station)
        
        if station_id:
            print(f"Getting baseline from station {station_id}")
            count, sequences, creatives = self.get_airplay_snapshot(station_id, start_date, end_date)
            
            if sequences:
                # Save the baseline sequence
                sequence_data = {
                    'last_airplay_sequence': sequences.get('airplay', 0),
                    'baseline_established': True,
                    'baseline_date': datetime.now().isoformat()
                }
                self.save_sequence(sequence_data)
                print(f"Baseline established with sequence: {sequences.get('airplay', 0)}")
                return True
        
        print("Failed to establish baseline")
        return False

    def get_airplay_changes(self, last_sequence):
        """Get airplay changes since last sequence (for future use)"""
        url = "https://data.mediamonitors.com/mmwebservices/service1.asmx/GetAirPlayChangesAfterSequenceString"
        params = {
            'username': self.username,
            'password': self.password,
            'sequenceID': last_sequence
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            root = ET.fromstring(response.text)
            
            records = []
            creatives = {}  # Use dict for deduplication by creative_id
            highest_sequence = last_sequence
            
            for elem in root.iter():
                if elem.tag.endswith('Table3'):
                    # Parse airplay record
                    record = {}
                    for child in elem:
                        tag_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        record[tag_name] = child.text
                    
                    if record:
                        records.append(record)
                        
                        # Extract creative data for new additions only
                        if record.get('action') == 'true':
                            creative_id = record.get('CreativeID')
                            aircheck_id = record.get('aircheck_id')
                            creative_name = record.get('Account_x002F_Title')
                            start_time_val = record.get('start_time')
                            end_time_val = record.get('end_time')
                            
                            if creative_id and creative_name:
                                creatives[creative_id] = {
                                    'creative_id': creative_id,
                                    'aircheck_id': aircheck_id,
                                    'creative_name': creative_name,
                                    'station_id': None,  # Not available in changes API
                                    'start_time': start_time_val,
                                    'end_time': end_time_val
                                }
                        
                        # Track highest sequence
                        if 'sequence_id' in record:
                            seq_id = int(record['sequence_id'])
                            highest_sequence = max(highest_sequence, seq_id)
            
            # Convert dict values back to list
            creatives_list = list(creatives.values())
            
            return records, highest_sequence, creatives_list
            
        except requests.RequestException as e:
            print(f"Error fetching airplay changes: {e}")
            return [], last_sequence, []
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return [], last_sequence, []

def main():
    tracker = MediaMonitorsTracker()
    
    print("Media Monitors Airplay Tracker - Enhanced with Deduplication")
    print("=" * 60)
    print(f"Mode: {'TEST' if TEST_MODE else 'PRODUCTION'}")
    print(f"Username: {USERNAME}")
    print(f"Date Range: {BASELINE_START_DATE} to {BASELINE_END_DATE}")
    print(f"Rate Limit Delay: {RATE_LIMIT_DELAY}s")
    print(f"Creatives Folder: {CREATIVES_FOLDER}")
    print(f"Next Script: {NEXT_SCRIPT}")
    print("=" * 60)
    
    # Get airplay data for the configured date range
    all_creatives = tracker.get_airplay_data()
    
    if all_creatives:
        # Filter out creatives that already exist
        new_creatives = tracker.filter_new_creatives(all_creatives)
        
        if new_creatives:
            # Save new creatives to JSON file
            filename = tracker.save_creatives(new_creatives, BASELINE_START_DATE, BASELINE_END_DATE)
            print(f"\nNew creatives saved to: {filename}")
            
            # Update master creative IDs list
            added_count = tracker.update_master_with_new_creatives(new_creatives)
            print(f"Added {added_count} new creative IDs to master list")
            
            # Show sample of new creative data for verification
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
            
            # Trigger next script
            success = tracker.run_next_script()
            if success:
                print(f"\n✓ Pipeline completed successfully!")
            else:
                print(f"\n✗ Pipeline completed with errors in next script")
        else:
            print(f"\nNo new creatives found - all {len(all_creatives)} creatives already exist in master list")
            print("Skipping file save and next script execution")
    else:
        print("No creatives found - skipping next script")

if __name__ == "__main__":
    main()