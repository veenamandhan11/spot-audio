import requests
import xml.etree.ElementTree as ET
import time
import json
import os
from datetime import datetime

class MediaMonitorsTracker:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.sequence_file = "last_sequence.json"
        self.creatives_file = "new_creatives.json"
        
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

    def get_airplay_snapshot_for_baseline(self, station_id, start_time, end_time):
        """Get airplay snapshot to establish baseline - returns count, sequences, and creative data"""
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
            creatives = []
            
            for elem in root.iter():
                if elem.tag.endswith('Table3'):
                    table3_count += 1
                    
                    # Extract creative information
                    creative_data = {}
                    for child in elem:
                        tag_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        creative_data[tag_name] = child.text
                    
                    # Add to creatives list if we have the key fields
                    if creative_data.get('CreativeID') and creative_data.get('Account_x002F_Title'):
                        creatives.append({
                            'creative_id': creative_data.get('CreativeID'),
                            'creative_name': creative_data.get('Account_x002F_Title')
                        })
            
            # Get sequence numbers
            sequences = {}
            for elem in root.iter():
                if elem.tag.endswith('BiggestSequenceForAirPlayChange'):
                    sequences['airplay'] = int(elem.text)
                elif elem.tag.endswith('BiggestSequenceForTitleAssignmentChange'):
                    sequences['title'] = int(elem.text)
                elif elem.tag.endswith('BiggestSequenceForMetaTitleIDChange'):
                    sequences['meta_title'] = int(elem.text)
            
            return table3_count, sequences, creatives
            
        except requests.RequestException as e:
            print(f"Error fetching snapshot for station {station_id}: {e}")
            return 0, {}, []
        except ET.ParseError as e:
            print(f"Error parsing XML for station {station_id}: {e}")
            return 0, {}, []

    def get_airplay_changes(self, last_sequence, max_rows=1000):
        """Get airplay changes since last sequence - returns records with creative data"""
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
            creatives = []
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
                        if record.get('action') == 'true' and record.get('CreativeID') and record.get('Account_x002F_Title'):
                            creatives.append({
                                'creative_id': record.get('CreativeID'),
                                'creative_name': record.get('Account_x002F_Title')
                            })
                        
                        # Track highest sequence
                        if 'sequence_id' in record:
                            seq_id = int(record['sequence_id'])
                            highest_sequence = max(highest_sequence, seq_id)
            
            return records, highest_sequence, creatives
            
        except requests.RequestException as e:
            print(f"Error fetching airplay changes: {e}")
            return [], last_sequence, []
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return [], last_sequence, []

    def save_sequence(self, sequence_data):
        """Save sequence data to file"""
        with open(self.sequence_file, 'w') as f:
            json.dump(sequence_data, f, indent=2)

    def save_creatives(self, creatives, timestamp=None):
        """Save creatives data to JSON file"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        output_data = {
            'timestamp': timestamp,
            'count': len(creatives),
            'creatives': creatives
        }
        
        with open(self.creatives_file, 'w') as f:
            json.dump(output_data, f, indent=2)

    def load_sequence(self):
        """Load sequence data from file"""
        if os.path.exists(self.sequence_file):
            with open(self.sequence_file, 'r') as f:
                return json.load(f)
        return None

    def establish_baseline(self, start_date="10/18/2025 00:00:00", end_date="10/18/2025 23:59:59"):
        """Establish baseline by getting snapshot of all stations"""
        print("Establishing baseline...")
        
        # Get all stations
        stations_xml = self.get_licensed_stations()
        if not stations_xml:
            print("Failed to get stations")
            return False
        
        # Parse stations
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
            
            print(f"Found {len(stations)} stations")
        except ET.ParseError as e:
            print(f"Error parsing stations XML: {e}")
            return False
        
        # Get baseline from first station to capture initial sequence
        if stations:
            first_station = stations[0]
            station_id_elem = None
            station_name_elem = None
            
            for child in first_station:
                if child.tag.endswith('StationID'):
                    station_id_elem = child
                elif child.tag.endswith('Station') and not child.tag.endswith('StationID'):
                    station_name_elem = child
            
            if station_id_elem is not None:
                station_id = station_id_elem.text
                station_name = station_name_elem.text if station_name_elem is not None else "Unknown"
                
                print(f"Getting baseline from station {station_id} ({station_name})...")
                count, sequences, creatives = self.get_airplay_snapshot_for_baseline(station_id, start_date, end_date)
                
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

    def get_new_records_and_creatives(self):
        """Get count and creative data of new records since last check"""
        sequence_data = self.load_sequence()
        
        if not sequence_data or not sequence_data.get('baseline_established'):
            print("No baseline found. Need to establish baseline first.")
            return 0, []
        
        last_sequence = sequence_data.get('last_airplay_sequence', 0)
        print(f"Checking for changes since sequence: {last_sequence}")
        
        total_new_count = 0
        all_new_creatives = []
        current_sequence = last_sequence
        
        # Keep fetching until we get all new records
        while True:
            records, highest_sequence, creatives = self.get_airplay_changes(current_sequence, max_rows=1000)
            
            if not records:
                break
            
            # Count only additions (action=true), not deletions
            new_additions = 0
            for record in records:
                if record.get('action') == 'true':
                    new_additions += 1
            
            total_new_count += new_additions
            all_new_creatives.extend(creatives)
            print(f"Batch: {len(records)} total records, {new_additions} new additions, {len(creatives)} new creatives")
            
            # Update sequence for next batch
            current_sequence = highest_sequence
            
            # If we got less than max_rows, we're done
            if len(records) < 1000:
                break
            
            # Rate limiting
            time.sleep(1)
        
        # Update saved sequence
        if current_sequence > last_sequence:
            sequence_data['last_airplay_sequence'] = current_sequence
            sequence_data['last_check'] = datetime.now().isoformat()
            self.save_sequence(sequence_data)
            print(f"Updated sequence to: {current_sequence}")
        
        return total_new_count, all_new_creatives

    def get_new_records_count(self):
        """Get count of new records since last check (backward compatibility)"""
        count, _ = self.get_new_records_and_creatives()
        return count

def main():
    # API credentials
    username = "CSugrue"
    password = "Ussu8229"
    
    tracker = MediaMonitorsTracker(username, password)
    
    print("Media Monitors New Records Tracker")
    print("=" * 50)
    
    # Check if baseline exists
    sequence_data = tracker.load_sequence()
    
    if not sequence_data or not sequence_data.get('baseline_established'):
        print("No baseline found. Establishing baseline...")
        if tracker.establish_baseline():
            print("Baseline established successfully!")
        else:
            print("Failed to establish baseline")
            return
    else:
        print(f"Baseline exists from: {sequence_data.get('baseline_date')}")
        print(f"Last sequence: {sequence_data.get('last_airplay_sequence')}")
    
    # Get new records count and creative data
    print("\nChecking for new records...")
    new_count, new_creatives = tracker.get_new_records_and_creatives()
    
    print(f"\n=== RESULTS ===")
    print(f"New airplay records since last check: {new_count}")
    print(f"New creatives found: {len(new_creatives)}")
    
    # Save creatives to JSON file
    if new_creatives:
        tracker.save_creatives(new_creatives)
        print(f"New creatives saved to {tracker.creatives_file}")
    else:
        print("No new creatives to save")

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