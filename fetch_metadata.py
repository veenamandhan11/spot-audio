import requests
import xml.etree.ElementTree as ET
import time
import json
import os
from datetime import datetime

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# API Credentials
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"

# File Paths
SEQUENCE_FILE = "last_sequence.json"
CREATIVES_FILE = "new_creatives.json"

# Baseline Configuration
BASELINE_START_DATE = "10/18/2025 00:00:00"
BASELINE_END_DATE = "10/18/2025 23:59:59"

# API Settings
MAX_ROWS_PER_REQUEST = 1000
RATE_LIMIT_DELAY = 1  # seconds between API calls

# Test Mode Configuration
TEST_MODE = True  # Set to True to test with first station only, False for production
TEST_CREATIVES_FILE = "test_creatives.json"  # Separate file for test output

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
        self.creatives_file = TEST_CREATIVES_FILE if TEST_MODE else CREATIVES_FILE
        self.test_mode = TEST_MODE
        
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
                    
                    # Add to creatives dict if we have the key fields (deduplicates automatically)
                    creative_id = creative_data.get('CreativeID')
                    creative_name = creative_data.get('Account_x002F_Title')
                    start_time_val = creative_data.get('start_time')
                    end_time_val = creative_data.get('end_time')
                    
                    if creative_id and creative_name:
                        creatives[creative_id] = {
                            'creative_id': creative_id,
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
            
            # Merge creatives into global dict (automatic deduplication)
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

    def save_creatives(self, creatives, timestamp=None):
        """Save creatives data to JSON file"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        output_data = {
            'timestamp': timestamp,
            'count': len(creatives),
            'test_mode': self.test_mode,
            'date_range': {
                'start': BASELINE_START_DATE,
                'end': BASELINE_END_DATE
            },
            'creatives': creatives
        }
        
        with open(self.creatives_file, 'w') as f:
            json.dump(output_data, f, indent=2)

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

    def get_airplay_changes(self, last_sequence, max_rows=MAX_ROWS_PER_REQUEST):
        """Get airplay changes since last sequence (for future use)"""
        url = "https://data.mediamonitors.com/mmwebservices/service1.asmx/GetAirPlayChangesAfterSequenceString"
        params = {
            'username': self.username,
            'password': self.password,
            'sequenceID': last_sequence,
            'maximum_rows': max_rows
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
                            creative_name = record.get('Account_x002F_Title')
                            start_time_val = record.get('start_time')
                            end_time_val = record.get('end_time')
                            
                            if creative_id and creative_name:
                                creatives[creative_id] = {
                                    'creative_id': creative_id,
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
    
    print("Media Monitors Airplay Tracker")
    print("=" * 50)
    print(f"Mode: {'TEST' if TEST_MODE else 'PRODUCTION'}")
    print(f"Username: {USERNAME}")
    print(f"Date Range: {BASELINE_START_DATE} to {BASELINE_END_DATE}")
    print(f"Rate Limit Delay: {RATE_LIMIT_DELAY}s")
    print("=" * 50)
    
    # Get airplay data for the configured date range
    creatives = tracker.get_airplay_data()
    
    # Save creatives to JSON file
    if creatives:
        tracker.save_creatives(creatives)
        print(f"\nCreatives saved to {tracker.creatives_file}")
        
        # Show sample of creative data for verification
        print("\nSample creative data:")
        for i, creative in enumerate(creatives[:3]):
            print(f"  {i+1}. ID: {creative['creative_id']}")
            print(f"      Station: {creative['station_id']}")
            print(f"      Name: {creative['creative_name']}")
            print(f"      Start: {creative['start_time']}")
            print(f"      End: {creative['end_time']}")
        
        if len(creatives) > 3:
            print(f"... and {len(creatives) - 3} more")
    else:
        print("No creatives found")

if __name__ == "__main__":
    main()


# import json
# from datetime import datetime

# def deduplicate_creatives_by_name(input_file, output_file):
#     """
#     Read JSON file with creatives, deduplicate by creative_name, and save clean version
#     """
#     try:
#         # Read the input JSON file
#         with open(input_file, 'r') as f:
#             data = json.load(f)
        
#         print(f"Original count: {data['count']}")
#         print(f"Original creatives in list: {len(data['creatives'])}")
        
#         # Use a dictionary to deduplicate by creative_name
#         # This will keep the first occurrence of each creative_name
#         unique_creatives = {}
        
#         for creative in data['creatives']:
#             creative_name = creative['creative_name']
#             if creative_name not in unique_creatives:
#                 unique_creatives[creative_name] = creative
        
#         # Convert back to list
#         deduplicated_list = list(unique_creatives.values())
        
#         # Create new clean data structure
#         clean_data = {
#             "timestamp": datetime.now().isoformat(),
#             "original_count": data['count'],
#             "original_timestamp": data['timestamp'],
#             "deduplicated_count": len(deduplicated_list),
#             "duplicates_removed": len(data['creatives']) - len(deduplicated_list),
#             "deduplication_method": "by_creative_name",
#             "creatives": deduplicated_list
#         }
        
#         # Save to new file
#         with open(output_file, 'w') as f:
#             json.dump(clean_data, f, indent=2)
        
#         print(f"Deduplicated count (by name): {len(deduplicated_list)}")
#         print(f"Duplicates removed: {len(data['creatives']) - len(deduplicated_list)}")
#         print(f"Clean data saved to: {output_file}")
        
#         return True
        
#     except FileNotFoundError:
#         print(f"Error: File '{input_file}' not found")
#         return False
#     except json.JSONDecodeError:
#         print(f"Error: Invalid JSON in file '{input_file}'")
#         return False
#     except Exception as e:
#         print(f"Error: {e}")
#         return False

# def deduplicate_both_methods(input_file, output_file_by_id, output_file_by_name):
#     """
#     Create both versions - deduplicated by ID and by name for comparison
#     """
#     try:
#         # Read the input JSON file
#         with open(input_file, 'r') as f:
#             data = json.load(f)
        
#         print(f"Original count: {data['count']}")
#         print(f"Original creatives in list: {len(data['creatives'])}")
        
#         # Deduplicate by creative_id
#         unique_by_id = {}
#         for creative in data['creatives']:
#             creative_id = creative['creative_id']
#             if creative_id not in unique_by_id:
#                 unique_by_id[creative_id] = creative
        
#         # Deduplicate by creative_name
#         unique_by_name = {}
#         for creative in data['creatives']:
#             creative_name = creative['creative_name']
#             if creative_name not in unique_by_name:
#                 unique_by_name[creative_name] = creative
        
#         # Convert to lists
#         deduplicated_by_id = list(unique_by_id.values())
#         deduplicated_by_name = list(unique_by_name.values())
        
#         # Create data structure for ID deduplication
#         clean_data_by_id = {
#             "timestamp": datetime.now().isoformat(),
#             "original_count": data['count'],
#             "original_timestamp": data['timestamp'],
#             "deduplicated_count": len(deduplicated_by_id),
#             "duplicates_removed": len(data['creatives']) - len(deduplicated_by_id),
#             "deduplication_method": "by_creative_id",
#             "creatives": deduplicated_by_id
#         }
        
#         # Create data structure for name deduplication
#         clean_data_by_name = {
#             "timestamp": datetime.now().isoformat(),
#             "original_count": data['count'],
#             "original_timestamp": data['timestamp'],
#             "deduplicated_count": len(deduplicated_by_name),
#             "duplicates_removed": len(data['creatives']) - len(deduplicated_by_name),
#             "deduplication_method": "by_creative_name",
#             "creatives": deduplicated_by_name
#         }
        
#         # Save both files
#         with open(output_file_by_id, 'w') as f:
#             json.dump(clean_data_by_id, f, indent=2)
        
#         with open(output_file_by_name, 'w') as f:
#             json.dump(clean_data_by_name, f, indent=2)
        
#         print(f"\nResults:")
#         print(f"Deduplicated by ID: {len(deduplicated_by_id)} unique creatives")
#         print(f"Deduplicated by Name: {len(deduplicated_by_name)} unique creatives")
#         print(f"Difference: {len(deduplicated_by_id) - len(deduplicated_by_name)} more unique IDs than names")
#         print(f"\nFiles saved:")
#         print(f"- By ID: {output_file_by_id}")
#         print(f"- By Name: {output_file_by_name}")
        
#         return True
        
#     except FileNotFoundError:
#         print(f"Error: File '{input_file}' not found")
#         return False
#     except json.JSONDecodeError:
#         print(f"Error: Invalid JSON in file '{input_file}'")
#         return False
#     except Exception as e:
#         print(f"Error: {e}")
#         return False

# def main():
#     input_file = "new_creatives.json"  # Your original file
#     output_file_by_name = "clean_creatives_by_name.json"  # Deduplicated by name
#     output_file_by_id = "clean_creatives_by_id.json"  # Deduplicated by ID
    
#     print("Creative Deduplication Tool")
#     print("=" * 40)
    
#     # Ask user which method they want
#     print("Choose deduplication method:")
#     print("1. Deduplicate by creative name only")
#     print("2. Create both versions (by ID and by name)")
    
#     choice = input("Enter choice (1 or 2): ").strip()
    
#     if choice == "1":
#         success = deduplicate_creatives_by_name(input_file, output_file_by_name)
#         if success:
#             print("\nDeduplication by name completed successfully!")
#     elif choice == "2":
#         success = deduplicate_both_methods(input_file, output_file_by_id, output_file_by_name)
#         if success:
#             print("\nBoth deduplication methods completed successfully!")
#     else:
#         print("Invalid choice. Please run again and select 1 or 2.")
#         return
    
#     if not success:
#         print("\nDeduplication failed!")

# if __name__ == "__main__":
#     main()