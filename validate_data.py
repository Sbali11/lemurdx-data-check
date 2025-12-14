#!/usr/bin/env python3
"""
Data Validation Script
Periodically downloads recent data and checks each modality for missing values and format.
"""

import os
import json
import boto3
import psycopg2
from datetime import datetime, timedelta
from botocore.config import Config
from dotenv import load_dotenv
from export import (
    load_all_config, get_db_connection, MEASURE_DEFINITIONS,
    query_timestream_measure_data, query_postgres_label_data
)

load_dotenv()

# Validation results file
VALIDATION_RESULTS_FILE = 'validation_results.json'
VALIDATION_HISTORY_FILE = 'validation_history.json'

# Format validation rules for each modality
FORMAT_RULES = {
    'motion_data': {
        'required_fields': ['acceleration_x', 'acceleration_y', 'acceleration_z'],
        'numeric_fields': [
            'acceleration_x', 'acceleration_y', 'acceleration_z',
            'gravity_x', 'gravity_y', 'gravity_z',
            'rotation_x', 'rotation_y', 'rotation_z',
            'mag_field_x', 'mag_field_y', 'mag_field_z',
            'roll', 'pitch', 'yaw',
            'quaternion_x', 'quaternion_y', 'quaternion_z', 'quaternion_w'
        ],
        'ranges': {
            'acceleration_x': (-20, 20),  # m/s² typical range
            'acceleration_y': (-20, 20),
            'acceleration_z': (-20, 20),
            'quaternion_x': (-1, 1),
            'quaternion_y': (-1, 1),
            'quaternion_z': (-1, 1),
            'quaternion_w': (-1, 1),
        }
    },
    'heart_rate_data': {
        'required_fields': ['heart_rate'],
        'numeric_fields': ['heart_rate'],
        'ranges': {
            'heart_rate': (30, 220)  # bpm typical range
        }
    },
    'location_data': {
        'required_fields': ['latitude', 'longitude'],
        'numeric_fields': ['latitude', 'longitude', 'altitude'],
        'ranges': {
            'latitude': (-90, 90),
            'longitude': (-180, 180),
            'altitude': (-500, 10000)  # meters
        }
    },
    'label_data': {
        'required_fields': ['device_id', 'uid', 'created_at'],
        'date_fields': ['created_at', 'completed_at'],
        'numeric_fields': ['enjoyment'],
        'ranges': {
            'enjoyment': (0, 10)  # Assuming 0-10 scale
        }
    }
}

def get_all_users_with_devices(db_config):
    """Get all users with their associated devices"""
    users_with_devices = []
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                # Get all users
                cursor.execute("""
                    SELECT DISTINCT u.id, u.email, u.type
                    FROM users u
                    JOIN users_watches uw ON u.id = uw.user_id
                    ORDER BY u.id ASC;
                """)
                user_results = cursor.fetchall()
                
                for user_row in user_results:
                    user_id, email, user_type = user_row
                    
                    # Get devices for this user
                    cursor.execute("""
                        SELECT w.hardware_id
                        FROM watches w
                        JOIN users_watches uw ON w.id = uw.watch_id
                        WHERE uw.user_id = %s
                        ORDER BY w.hardware_id ASC;
                    """, (user_id,))
                    device_results = cursor.fetchall()
                    
                    devices = [row[0] for row in device_results]
                    
                    if devices:
                        users_with_devices.append({
                            'user_id': user_id,
                            'email': email,
                            'type': user_type,
                            'devices': devices
                        })
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    return users_with_devices

def validate_format(measure_name, field_name, value):
    """Validate format of a field value based on modality rules"""
    errors = []
    
    if measure_name not in FORMAT_RULES:
        return errors
    
    rules = FORMAT_RULES[measure_name]
    
    # Check if numeric field is actually numeric
    if field_name in rules.get('numeric_fields', []):
        try:
            num_value = float(value)
            
            # Check if value is within expected range
            if field_name in rules.get('ranges', {}):
                min_val, max_val = rules['ranges'][field_name]
                if num_value < min_val or num_value > max_val:
                    errors.append(f'Value {num_value} outside expected range [{min_val}, {max_val}]')
        except (ValueError, TypeError):
            errors.append(f'Not a valid number: {value}')
    
    # Check required fields
    if field_name in rules.get('required_fields', []):
        if value is None or str(value).strip() == '':
            errors.append('Required field is missing or empty')
    
    return errors

def calculate_sampling_stats(data_rows, measure_name):
    """Calculate actual sampling frequency and detect gaps from data"""
    if not data_rows or len(data_rows) < 2:
        return {
            'sampling_frequency': None,
            'sampling_period': None,
            'gaps': [],
            'total_gap_time': None
        }
    
    # Extract timestamps
    timestamps = []
    for row in data_rows:
        if 'time' in row and row['time']:
            try:
                # Parse timestamp
                ts_str = str(row['time'])
                if 'T' in ts_str:
                    ts_str = ts_str.replace('T', ' ')
                # Try parsing
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S']:
                    try:
                        ts = datetime.strptime(ts_str.split('.')[0].split('+')[0].split('Z')[0].strip(), fmt)
                        timestamps.append(ts)
                        break
                    except ValueError:
                        continue
            except Exception:
                continue
    
    if len(timestamps) < 2:
        return {
            'sampling_frequency': None,
            'sampling_period': None,
            'gaps': [],
            'total_gap_time': None
        }
    
    # Sort timestamps
    timestamps.sort()
    
    # Calculate time differences
    time_diffs = []
    for i in range(1, len(timestamps)):
        diff = (timestamps[i] - timestamps[i-1]).total_seconds()
        time_diffs.append(diff)
    
    if not time_diffs:
        return {
            'sampling_frequency': None,
            'sampling_period': None,
            'gaps': [],
            'total_gap_time': None
        }
    
    # Calculate median sampling period (more robust than mean)
    sorted_diffs = sorted(time_diffs)
    median_idx = len(sorted_diffs) // 2
    median_period = sorted_diffs[median_idx] if len(sorted_diffs) % 2 == 1 else (sorted_diffs[median_idx-1] + sorted_diffs[median_idx]) / 2
    
    # Calculate average sampling frequency
    avg_period = sum(time_diffs) / len(time_diffs)
    sampling_frequency = 1.0 / avg_period if avg_period > 0 else None
    
    # Detect gaps (periods significantly longer than median)
    # Gap threshold: 3x the median period
    gap_threshold = median_period * 3
    gaps = []
    total_gap_seconds = 0
    
    for i in range(1, len(timestamps)):
        diff_seconds = (timestamps[i] - timestamps[i-1]).total_seconds()
        if diff_seconds > gap_threshold:
            gap_info = {
                'start': timestamps[i-1].isoformat(),
                'end': timestamps[i].isoformat(),
                'duration_seconds': diff_seconds,
                'duration_formatted': format_duration(diff_seconds)
            }
            gaps.append(gap_info)
            total_gap_seconds += diff_seconds
    
    return {
        'sampling_frequency': sampling_frequency,
        'sampling_period': avg_period,
        'median_period': median_period,
        'gaps': gaps,
        'total_gap_time': total_gap_seconds,
        'total_gap_time_formatted': format_duration(total_gap_seconds) if total_gap_seconds > 0 else None,
        'gap_count': len(gaps)
    }

def format_duration(seconds):
    """Format duration in seconds to human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"

def validate_timestream_data(ts_query_client, config, device_id, measure_name, days_back=7):
    """Validate Timestream data for a device and measure"""
    validation_result = {
        'has_data': False,
        'total_rows': 0,
        'missing_values': {},
        'format_errors': [],
        'format_valid': True,
        'date_range': None,
        'sampling_stats': None
    }
    
    try:
        # Get date range
        from export import get_timestream_date_range
        min_time, max_time = get_timestream_date_range(ts_query_client, config, device_id, measure_name)
        
        if min_time is None or max_time is None:
            return validation_result
        
        validation_result['date_range'] = {
            'min': min_time,
            'max': max_time
        }
        
        # Get recent data (last N days or all if less)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days_back)
        
        # Use the actual min_time if it's more recent than days_back
        min_dt_str = min_time.replace('Z', '+00:00') if 'Z' in min_time else min_time
        try:
            min_dt = datetime.fromisoformat(min_dt_str)
        except ValueError:
            # Try alternative parsing
            min_dt = datetime.strptime(min_dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        if min_dt > start_time:
            start_time = min_dt
        
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        sub_measure_names = MEASURE_DEFINITIONS.get(measure_name, [])
        
        # Query data
        data_rows = query_timestream_measure_data(
            ts_query_client, config, device_id, measure_name,
            sub_measure_names, start_time_str, end_time_str
        )
        
        if not data_rows:
            return validation_result
        
        validation_result['has_data'] = True
        validation_result['total_rows'] = len(data_rows)
        
        # Calculate sampling statistics and gaps
        sampling_stats = calculate_sampling_stats(data_rows, measure_name)
        validation_result['sampling_stats'] = sampling_stats
        
        # Get format rules for this modality
        format_rules = FORMAT_RULES.get(measure_name, {})
        
        # Check for missing values in required fields
        required_fields = format_rules.get('required_fields', [])
        for field in required_fields:
            if field in sub_measure_names:
                missing_count = sum(1 for row in data_rows if field not in row or row[field] is None or str(row[field]).strip() == '')
                if missing_count > 0:
                    validation_result['missing_values'][field] = {
                        'count': missing_count,
                        'percentage': (missing_count / len(data_rows)) * 100
                    }
        
        # Check format for all fields
        format_errors_count = 0
        for i, row in enumerate(data_rows):
            # Limit format checking to first 1000 rows for performance
            if i >= 1000:
                break
                
            for field in sub_measure_names:
                if field in row and row[field] is not None:
                    errors = validate_format(measure_name, field, row[field])
                    if errors:
                        format_errors_count += 1
                        validation_result['format_errors'].append({
                            'row': i,
                            'field': field,
                            'value': str(row[field]),
                            'errors': errors
                        })
        
        # Mark format as invalid if errors found
        if validation_result['format_errors']:
            validation_result['format_valid'] = False
        
        # Add summary
        validation_result['format_errors_count'] = format_errors_count
        validation_result['format_errors_percentage'] = (format_errors_count / min(len(data_rows), 1000)) * 100 if data_rows else 0
        
    except Exception as e:
        validation_result['error'] = str(e)
    
    return validation_result

def validate_postgres_label_data(db_config, device_id):
    """Validate PostgreSQL label data for a device"""
    validation_result = {
        'has_data': False,
        'total_rows': 0,
        'missing_values': {},
        'format_errors': [],
        'format_valid': True,
        'date_range': None,
        'sampling_stats': None
    }
    
    try:
        from export import get_postgres_label_date_range
        min_time, max_time = get_postgres_label_date_range(db_config, device_id)
        
        if min_time is None or max_time is None:
            return validation_result
        
        validation_result['date_range'] = {
            'min': min_time,
            'max': max_time
        }
        
        sub_measure_names = MEASURE_DEFINITIONS.get('label_data', [])
        data_rows = query_postgres_label_data(db_config, device_id, sub_measure_names)
        
        if not data_rows:
            return validation_result
        
        validation_result['has_data'] = True
        validation_result['total_rows'] = len(data_rows)
        
        # Calculate sampling statistics (for label data, this will show event frequency)
        sampling_stats = calculate_sampling_stats(data_rows, 'label_data')
        validation_result['sampling_stats'] = sampling_stats
        
        # Get format rules
        format_rules = FORMAT_RULES.get('label_data', {})
        required_fields = format_rules.get('required_fields', [])
        
        # Check for missing values in required fields
        for field in required_fields:
            if field in sub_measure_names:
                missing_count = sum(1 for row in data_rows if field not in row or row[field] is None or str(row[field]).strip() == '')
                if missing_count > 0:
                    validation_result['missing_values'][field] = {
                        'count': missing_count,
                        'percentage': (missing_count / len(data_rows)) * 100
                    }
        
        # Check format for all fields
        format_errors_count = 0
        for i, row in enumerate(data_rows):
            for field in sub_measure_names:
                if field in row and row[field] is not None:
                    errors = validate_format('label_data', field, row[field])
                    if errors:
                        format_errors_count += 1
                        validation_result['format_errors'].append({
                            'row': i,
                            'field': field,
                            'value': str(row[field]),
                            'errors': errors
                        })
        
        # Mark format as invalid if errors found
        if validation_result['format_errors']:
            validation_result['format_valid'] = False
        
        validation_result['format_errors_count'] = format_errors_count
        validation_result['format_errors_percentage'] = (format_errors_count / len(data_rows)) * 100 if data_rows else 0
        
    except Exception as e:
        validation_result['error'] = str(e)
    
    return validation_result

def run_validation(days_back=7):
    """Run validation for all users and devices"""
    print(f"Starting data validation at {datetime.now()}")
    
    config = load_all_config()
    
    # Validate configurations
    if not all(config["db"].values()):
        raise ValueError("Database configuration is not set")
    
    if not config["timestream"]["database"] or not config["timestream"]["table"]:
        raise ValueError("Timestream configuration is not set")
    
    # Get all users with devices
    print("Fetching users and devices...")
    users_with_devices = get_all_users_with_devices(config["db"])
    print(f"Found {len(users_with_devices)} users with devices")
    
    # Initialize Timestream client
    ts_query_client = boto3.client('timestream-query', config=Config(read_timeout=30, retries={'max_attempts': 5}))
    
    # Validation results structure
    results = {
        'last_run': datetime.now().isoformat(),
        'days_back': days_back,
        'users': []
    }
    
    # Process each user
    for user_info in users_with_devices:
        user_id = user_info['user_id']
        email = user_info['email']
        devices = user_info['devices']
        
        print(f"\nValidating data for user {email} (ID: {user_id})")
        
        user_result = {
            'user_id': user_id,
            'email': email,
            'type': user_info['type'],
            'devices': []
        }
        
        # Process each device
        for device_id in devices:
            print(f"  Checking device {device_id}...")
            
            device_result = {
                'device_id': device_id,
                'modalities': {}
            }
            
            # Validate each modality
            for measure_name in MEASURE_DEFINITIONS.keys():
                print(f"    Validating {measure_name}...")
                
                if measure_name == 'label_data':
                    modality_result = validate_postgres_label_data(config["db"], device_id)
                else:
                    modality_result = validate_timestream_data(
                        ts_query_client, config, device_id, measure_name, days_back
                    )
                
                device_result['modalities'][measure_name] = modality_result
            
            user_result['devices'].append(device_result)
        
        results['users'].append(user_result)
    
    # Save results to file
    with open(VALIDATION_RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Save to history (accumulate date ranges)
    save_validation_history(results)
    
    print(f"\nValidation complete! Results saved to {VALIDATION_RESULTS_FILE}")
    print(f"History updated in {VALIDATION_HISTORY_FILE}")
    return results

def load_validation_results():
    """Load validation results from file"""
    if not os.path.exists(VALIDATION_RESULTS_FILE):
        return None
    
    try:
        with open(VALIDATION_RESULTS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading validation results: {e}")
        return None

def load_validation_history():
    """Load validation history from file"""
    if not os.path.exists(VALIDATION_HISTORY_FILE):
        return {}
    
    try:
        with open(VALIDATION_HISTORY_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading validation history: {e}")
        return {}

def save_validation_history(results):
    """Save validation results to history, accumulating date ranges per user"""
    history = load_validation_history()
    
    run_timestamp = results.get('last_run', datetime.now().isoformat())
    
    # Process each user
    for user_info in results.get('users', []):
        user_id = str(user_info['user_id'])
        email = user_info['email']
        
        if user_id not in history:
            history[user_id] = {
                'email': email,
                'type': user_info.get('type'),
                'date_ranges_checked': [],
                'devices': {}
            }
        
        # Update user info
        history[user_id]['email'] = email
        history[user_id]['type'] = user_info.get('type')
        
        # Process devices
        for device_info in user_info.get('devices', []):
            device_id = str(device_info['device_id'])
            
            if device_id not in history[user_id]['devices']:
                history[user_id]['devices'][device_id] = {
                    'date_ranges_checked': [],
                    'modalities': {}
                }
            
            # Process modalities
            for modality_name, modality_result in device_info.get('modalities', {}).items():
                if modality_name not in history[user_id]['devices'][device_id]['modalities']:
                    history[user_id]['devices'][device_id]['modalities'][modality_name] = {
                        'date_ranges_checked': []
                    }
                
                # Add date range if it exists
                if modality_result.get('date_range'):
                    date_range = modality_result['date_range']
                    date_range_entry = {
                        'run_timestamp': run_timestamp,
                        'start': date_range.get('min'),
                        'end': date_range.get('max'),
                        'has_data': modality_result.get('has_data', False),
                        'total_rows': modality_result.get('total_rows', 0)
                    }
                    
                    # Check if this range already exists (avoid duplicates)
                    existing = history[user_id]['devices'][device_id]['modalities'][modality_name]['date_ranges_checked']
                    is_duplicate = any(
                        dr.get('start') == date_range_entry['start'] and 
                        dr.get('end') == date_range_entry['end']
                        for dr in existing
                    )
                    
                    if not is_duplicate:
                        existing.append(date_range_entry)
                
                # Add to device-level date ranges
                if modality_result.get('date_range'):
                    date_range = modality_result['date_range']
                    device_range_entry = {
                        'run_timestamp': run_timestamp,
                        'modality': modality_name,
                        'start': date_range.get('min'),
                        'end': date_range.get('max')
                    }
                    
                    existing_device_ranges = history[user_id]['devices'][device_id]['date_ranges_checked']
                    is_duplicate = any(
                        dr.get('start') == device_range_entry['start'] and 
                        dr.get('end') == device_range_entry['end'] and
                        dr.get('modality') == modality_name
                        for dr in existing_device_ranges
                    )
                    
                    if not is_duplicate:
                        existing_device_ranges.append(device_range_entry)
            
            # Add to user-level date ranges
            for modality_name, modality_result in device_info.get('modalities', {}).items():
                if modality_result.get('date_range'):
                    date_range = modality_result['date_range']
                    user_range_entry = {
                        'run_timestamp': run_timestamp,
                        'device_id': device_id,
                        'modality': modality_name,
                        'start': date_range.get('min'),
                        'end': date_range.get('max')
                    }
                    
                    existing_user_ranges = history[user_id]['date_ranges_checked']
                    is_duplicate = any(
                        dr.get('start') == user_range_entry['start'] and 
                        dr.get('end') == user_range_entry['end'] and
                        dr.get('device_id') == device_id and
                        dr.get('modality') == modality_name
                        for dr in existing_user_ranges
                    )
                    
                    if not is_duplicate:
                        existing_user_ranges.append(user_range_entry)
    
    # Save history
    try:
        with open(VALIDATION_HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving validation history: {e}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate data for all users and devices')
    parser.add_argument('--days-back', type=int, default=1, help='Number of days back to check (default: 7)')
    
    args = parser.parse_args()
    
    try:
        results = run_validation(days_back=args.days_back)
        print(f"\nValidation completed successfully!")
        print(f"Total users checked: {len(results['users'])}")
    except Exception as e:
        print(f"Validation failed: {e}")
        exit(1)

