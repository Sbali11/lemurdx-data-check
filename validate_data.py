#!/usr/bin/env python3
"""
Data Validation Script
Periodically downloads recent data and checks each modality for missing values and format.
"""

import os
import json
import boto3
import psycopg2
import threading
from datetime import datetime, timedelta
from botocore.config import Config
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from export import (
    load_all_config, get_db_connection, MEASURE_DEFINITIONS,
    query_timestream_measure_data, query_postgres_label_data,
    _paginate_timestream_query, parse_timestream_response
)

load_dotenv()

# Validation results file
VALIDATION_RESULTS_FILE = 'validation_results.json'
VALIDATION_HISTORY_FILE = 'validation_history.json'

# Thread-safe lock for saving user results
_save_lock = threading.Lock()

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

def validate_timestream_data(ts_query_client, config, device_id, measure_name, start_time=None, end_time=None):
    """Validate Timestream data for a device and measure
    
    Args:
        start_time: Start datetime for validation (if None, uses all available data from beginning)
        end_time: End datetime for validation (if None, uses current time)
    """
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
        # Get date range of all available data
        from export import get_timestream_date_range
        min_time, max_time = get_timestream_date_range(ts_query_client, config, device_id, measure_name)
        
        if min_time is None or max_time is None:
            return validation_result
        
        # Determine validation time range
        if end_time is None:
            end_time = datetime.utcnow()
        else:
            # Parse end_time if it's a string
            if isinstance(end_time, str):
                try:
                    end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00') if 'Z' in end_time else end_time)
                except ValueError:
                    end_time = datetime.strptime(end_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        if start_time is None:
            # Validate all available data from beginning
            min_dt_str = min_time.replace('Z', '+00:00') if 'Z' in min_time else min_time
            try:
                start_time = datetime.fromisoformat(min_dt_str)
            except ValueError:
                start_time = datetime.strptime(min_dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        else:
            # Parse start_time if it's a string
            if isinstance(start_time, str):
                try:
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00') if 'Z' in start_time else start_time)
                except ValueError:
                    start_time = datetime.strptime(start_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        # Store the actual date range being validated
        validation_result['date_range'] = {
            'min': start_time.isoformat(),
            'max': end_time.isoformat()
        }
        
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        sub_measure_names = MEASURE_DEFINITIONS.get(measure_name, [])
        
        # For large date ranges, use sampling to avoid exceeding query result size limits
        # Calculate time span in days
        time_span_days = (end_time - start_time).total_seconds() / (24 * 3600)
        
        # If time span is large (>30 days), use sampling approach
        if time_span_days > 30:
            print(f"[Thread]       Large time range ({time_span_days:.1f} days), using sampling approach")
            # Use aggregation query to get total count first
            count_query = f"""
                SELECT COUNT(*) as total_count
                FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                WHERE measure_name = '{measure_name}'
                  AND device_id = '{device_id}'
                  AND time BETWEEN TIMESTAMP '{start_time_str}' AND TIMESTAMP '{end_time_str}'
            """
            
            try:
                count_response = _paginate_timestream_query(ts_query_client, count_query)
                count_rows = parse_timestream_response(count_response)
                total_count = int(count_rows[0]['total_count']) if count_rows else 0
                
                if total_count == 0:
                    return validation_result
                
                validation_result['has_data'] = True
                validation_result['total_rows'] = total_count
                
                # Sample data for validation (limit to reasonable size)
                # Sample every Nth row or use time-based sampling
                sample_interval = max(1, total_count // 10000)  # Sample up to 10k rows
                
                # Use TABLESAMPLE or LIMIT with OFFSET for sampling
                # Since Timestream doesn't support TABLESAMPLE, we'll use a time-based approach
                # Sample evenly across the time range
                num_samples = min(10000, total_count)
                time_step = (end_time - start_time) / num_samples if num_samples > 0 else timedelta(seconds=1)
                
                # Build query to sample data points evenly across time range
                select_measures_sql = ", ".join([f"{name}" for name in sub_measure_names])
                sample_query = f"""
                    SELECT time, {select_measures_sql}
                    FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                    WHERE measure_name = '{measure_name}'
                      AND device_id = '{device_id}'
                      AND time BETWEEN TIMESTAMP '{start_time_str}' AND TIMESTAMP '{end_time_str}'
                    ORDER BY time ASC
                    LIMIT 10000
                """
                
                print(f"[Thread]       Sampling {num_samples} rows from {total_count} total rows")
                sample_response = _paginate_timestream_query(ts_query_client, sample_query)
                data_rows = parse_timestream_response(sample_response)
                
                if not data_rows:
                    # If sampling fails, try with smaller limit
                    sample_query = sample_query.replace('LIMIT 10000', 'LIMIT 1000')
                    sample_response = _paginate_timestream_query(ts_query_client, sample_query)
                    data_rows = parse_timestream_response(sample_response)
                
                # Note that we're using sampled data for validation
                validation_result['sampled'] = True
                validation_result['sample_size'] = len(data_rows)
                validation_result['total_rows'] = total_count
                
            except Exception as e:
                # If aggregation fails, fall back to smaller time chunks
                print(f"[Thread]       Aggregation failed, using time-chunked approach: {e}")
                return validate_timestream_data_chunked(
                    ts_query_client, config, device_id, measure_name, start_time, end_time, validation_result
                )
        else:
            # For smaller ranges, query normally but with a limit
            select_measures_sql = ", ".join([f"{name}" for name in sub_measure_names])
            query = f"""
                SELECT time, {select_measures_sql}
                FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                WHERE measure_name = '{measure_name}'
                  AND device_id = '{device_id}'
                  AND time BETWEEN TIMESTAMP '{start_time_str}' AND TIMESTAMP '{end_time_str}'
                ORDER BY time ASC
                LIMIT 50000
            """
            
            try:
                response = _paginate_timestream_query(ts_query_client, query)
                data_rows = parse_timestream_response(response)
                
                if not data_rows:
                    return validation_result
                
                validation_result['has_data'] = True
                validation_result['total_rows'] = len(data_rows)
                
            except Exception as e:
                error_msg = str(e)
                if 'max query result size' in error_msg.lower() or 'query aborted' in error_msg.lower():
                    # Fall back to chunked approach
                    print(f"[Thread]       Query size limit exceeded, using time-chunked approach")
                    return validate_timestream_data_chunked(
                        ts_query_client, config, device_id, measure_name, start_time, end_time, validation_result
                    )
                else:
                    raise
        
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
        error_msg = str(e)
        if 'max query result size' in error_msg.lower() or 'query aborted' in error_msg.lower():
            # Try chunked approach as fallback
            print(f"[Thread]       Query failed due to size limit, trying chunked approach")
            try:
                return validate_timestream_data_chunked(
                    ts_query_client, config, device_id, measure_name, start_time, end_time, validation_result
                )
            except Exception as chunk_error:
                validation_result['error'] = f"Original error: {error_msg}. Chunked approach also failed: {str(chunk_error)}"
        else:
            validation_result['error'] = error_msg
    
    return validation_result

def validate_timestream_data_chunked(ts_query_client, config, device_id, measure_name, start_time, end_time, validation_result):
    """Validate Timestream data using time-chunked queries to avoid size limits"""
    sub_measure_names = MEASURE_DEFINITIONS.get(measure_name, [])
    select_measures_sql = ", ".join([f"{name}" for name in sub_measure_names])
    
    # Chunk by months to avoid size limits
    chunk_size_days = 30
    current_start = start_time
    all_data_rows = []
    total_count = 0
    
    print(f"[Thread]       Processing in {chunk_size_days}-day chunks from {start_time} to {end_time}")
    
    while current_start < end_time:
        chunk_end = min(current_start + timedelta(days=chunk_size_days), end_time)
        start_str = current_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = chunk_end.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Get count for this chunk
            count_query = f"""
                SELECT COUNT(*) as total_count
                FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                WHERE measure_name = '{measure_name}'
                  AND device_id = '{device_id}'
                  AND time BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
            """
            count_response = _paginate_timestream_query(ts_query_client, count_query)
            count_rows = parse_timestream_response(count_response)
            chunk_count = int(count_rows[0]['total_count']) if count_rows else 0
            total_count += chunk_count
            
            # Sample data from this chunk (up to 2000 rows per chunk)
            sample_limit = min(2000, chunk_count) if chunk_count > 0 else 0
            if sample_limit > 0:
                query = f"""
                    SELECT time, {select_measures_sql}
                    FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                    WHERE measure_name = '{measure_name}'
                      AND device_id = '{device_id}'
                      AND time BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
                    ORDER BY time ASC
                    LIMIT {sample_limit}
                """
                response = _paginate_timestream_query(ts_query_client, query)
                chunk_rows = parse_timestream_response(response)
                all_data_rows.extend(chunk_rows)
                
        except Exception as e:
            print(f"[Thread]       Error processing chunk {start_str} to {end_str}: {e}")
            # Continue with next chunk
        
        current_start = chunk_end
    
    if total_count == 0:
        return validation_result
    
    validation_result['has_data'] = True
    validation_result['total_rows'] = total_count
    validation_result['sampled'] = True
    validation_result['sample_size'] = len(all_data_rows)
    
    # Continue with validation using sampled data
    data_rows = all_data_rows
    
    if not data_rows:
        return validation_result
    
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
    
    # Check format for all fields (limited to sampled data)
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
    
    return validation_result

def validate_timestream_data_chunked(ts_query_client, config, device_id, measure_name, start_time, end_time, validation_result):
    """Validate Timestream data using time-chunked queries to avoid size limits"""
    sub_measure_names = MEASURE_DEFINITIONS.get(measure_name, [])
    select_measures_sql = ", ".join([f"{name}" for name in sub_measure_names])
    
    # Chunk by months to avoid size limits
    chunk_size_days = 30
    current_start = start_time
    all_data_rows = []
    total_count = 0
    
    print(f"[Thread]       Processing in {chunk_size_days}-day chunks from {start_time} to {end_time}")
    
    while current_start < end_time:
        chunk_end = min(current_start + timedelta(days=chunk_size_days), end_time)
        start_str = current_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = chunk_end.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Get count for this chunk
            count_query = f"""
                SELECT COUNT(*) as total_count
                FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                WHERE measure_name = '{measure_name}'
                  AND device_id = '{device_id}'
                  AND time BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
            """
            count_response = _paginate_timestream_query(ts_query_client, count_query)
            count_rows = parse_timestream_response(count_response)
            chunk_count = int(count_rows[0]['total_count']) if count_rows else 0
            total_count += chunk_count
            
            # Sample data from this chunk (up to 2000 rows per chunk)
            sample_limit = min(2000, chunk_count) if chunk_count > 0 else 0
            if sample_limit > 0:
                query = f"""
                    SELECT time, {select_measures_sql}
                    FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
                    WHERE measure_name = '{measure_name}'
                      AND device_id = '{device_id}'
                      AND time BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
                    ORDER BY time ASC
                    LIMIT {sample_limit}
                """
                response = _paginate_timestream_query(ts_query_client, query)
                chunk_rows = parse_timestream_response(response)
                all_data_rows.extend(chunk_rows)
                
        except Exception as e:
            print(f"[Thread]       Error processing chunk {start_str} to {end_str}: {e}")
            # Continue with next chunk
        
        current_start = chunk_end
    
    if total_count == 0:
        return validation_result
    
    validation_result['has_data'] = True
    validation_result['total_rows'] = total_count
    validation_result['sampled'] = True
    validation_result['sample_size'] = len(all_data_rows)
    
    # Continue with validation using sampled data
    data_rows = all_data_rows
    
    if not data_rows:
        return validation_result
    
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
    
    # Check format for all fields (limited to sampled data)
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
    
    return validation_result

def validate_postgres_label_data(db_config, device_id, start_time=None, end_time=None):
    """Validate PostgreSQL label data for a device
    
    Args:
        start_time: Start datetime for validation (if None, uses all available data from beginning)
        end_time: End datetime for validation (if None, uses current time)
    """
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
        
        # Determine validation time range
        if end_time is None:
            end_time = datetime.utcnow()
        else:
            if isinstance(end_time, str):
                try:
                    end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00') if 'Z' in end_time else end_time)
                except ValueError:
                    end_time = datetime.strptime(end_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        if start_time is None:
            # Validate all available data from beginning
            try:
                start_time = datetime.fromisoformat(min_time.replace('Z', '+00:00') if 'Z' in min_time else min_time)
            except ValueError:
                start_time = datetime.strptime(min_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
        else:
            if isinstance(start_time, str):
                try:
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00') if 'Z' in start_time else start_time)
                except ValueError:
                    start_time = datetime.strptime(start_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
        
        validation_result['date_range'] = {
            'min': start_time.isoformat(),
            'max': end_time.isoformat()
        }
        
        sub_measure_names = MEASURE_DEFINITIONS.get('label_data', [])
        data_rows = query_postgres_label_data(db_config, device_id, sub_measure_names)
        
        # Filter data rows by time range if needed
        if start_time or end_time:
            filtered_rows = []
            for row in data_rows:
                row_time = None
                # Try to get time from created_at or completed_at
                for time_field in ['created_at', 'completed_at', 'time']:
                    if time_field in row and row[time_field]:
                        try:
                            time_str = str(row[time_field])
                            if 'T' in time_str:
                                time_str = time_str.replace('T', ' ')
                            row_time = datetime.strptime(time_str.split('.')[0].split('+')[0].split('Z')[0].strip(), '%Y-%m-%d %H:%M:%S')
                            break
                        except (ValueError, TypeError):
                            continue
                
                if row_time:
                    if start_time and row_time < start_time:
                        continue
                    if end_time and row_time > end_time:
                        continue
                
                filtered_rows.append(row)
            data_rows = filtered_rows
        
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

def get_latest_validation_date(history, user_id, device_id, modality):
    """Get the latest end date for a user/device/modality from history"""
    user_id_str = str(user_id)
    device_id_str = str(device_id)
    
    if user_id_str not in history:
        return None
    
    user_history = history[user_id_str]
    
    # Check device-level history
    if 'devices' in user_history and device_id_str in user_history['devices']:
        device_history = user_history['devices'][device_id_str]
        if 'modalities' in device_history and modality in device_history['modalities']:
            modality_history = device_history['modalities'][modality]
            if 'date_ranges_checked' in modality_history and modality_history['date_ranges_checked']:
                # Get the latest end date
                ranges = modality_history['date_ranges_checked']
                latest_range = max(ranges, key=lambda x: x.get('end', ''))
                return latest_range.get('end')
    
    return None

# Thread-safe lock for saving user results
_save_lock = threading.Lock()

def save_user_result(results_file, user_result, run_timestamp):
    """Save a single user's validation result to file (thread-safe)"""
    global _save_lock
    
    try:
        with _save_lock:
            # Load existing results
            existing_results = {}
            if os.path.exists(results_file):
                try:
                    with open(results_file, 'r') as f:
                        existing_results = json.load(f)
                except Exception as e:
                    print(f"[Thread] Warning: Could not load existing results: {e}", flush=True)
            
            # Update or add user result
            if 'users' not in existing_results:
                existing_results['users'] = []
            
            # Remove existing entry for this user if present (update existing user)
            existing_results['users'] = [
                u for u in existing_results['users'] 
                if str(u.get('user_id')) != str(user_result.get('user_id'))
            ]
            
            # Add new/updated user result
            existing_results['users'].append(user_result)
            
            # Update metadata
            existing_results['last_run'] = run_timestamp
            existing_results['validation_strategy'] = 'incremental'
            if 'parallel_workers' not in existing_results:
                existing_results['parallel_workers'] = 4  # Default, will be updated
            
            # Save back to file
            try:
                # Ensure directory exists
                results_dir = os.path.dirname(os.path.abspath(results_file))
                if results_dir and not os.path.exists(results_dir):
                    os.makedirs(results_dir, exist_ok=True)
                
                with open(results_file, 'w') as f:
                    json.dump(existing_results, f, indent=2, default=str)
                print(f"[Thread] ✓ Saved results for user {user_result.get('email', 'unknown')} ({len(existing_results['users'])} total users)", flush=True)
            except Exception as e:
                import traceback
                print(f"[Thread] ✗ Error saving results for user {user_result.get('email', 'unknown')}: {e}", flush=True)
                print(f"[Thread] Traceback: {traceback.format_exc()}", flush=True)
                raise
    except Exception as e:
        import traceback
        print(f"[Thread] ✗ Critical error in save_user_result: {e}", flush=True)
        print(f"[Thread] Traceback: {traceback.format_exc()}", flush=True)
        raise

def validate_single_user(user_info, config, history, run_timestamp, results_file):
    """Validate data for a single user (can be called in parallel)"""
    user_id = user_info['user_id']
    email = user_info['email']
    devices = user_info['devices']
    
    print(f"[Thread] Validating data for user {email} (ID: {user_id})", flush=True)
    
    # Create Timestream client for this thread (longer timeout for large queries)
    ts_query_client = boto3.client('timestream-query', config=Config(read_timeout=300, retries={'max_attempts': 3}))
    
    user_result = {
        'user_id': user_id,
        'email': email,
        'type': user_info['type'],
        'devices': []
    }
    
    try:
        # Process each device
        for device_id in devices:
            print(f"[Thread]   Checking device {device_id} for user {email}...", flush=True)
            
            device_result = {
                'device_id': device_id,
                'modalities': {}
            }
            
            # Validate each modality
            for measure_name in MEASURE_DEFINITIONS.keys():
                print(f"[Thread]     Validating {measure_name} for device {device_id}...", flush=True)
                
                # Determine start time based on history
                latest_end_date = get_latest_validation_date(history, user_id, device_id, measure_name)
                start_time = None
                
                if latest_end_date:
                    # User/device/modality exists in history - validate from last check to now
                    print(f"[Thread]       Found previous validation, checking from {latest_end_date} to now", flush=True)
                    try:
                        start_time = datetime.fromisoformat(latest_end_date.replace('Z', '+00:00') if 'Z' in latest_end_date else latest_end_date)
                    except ValueError:
                        start_time = datetime.strptime(latest_end_date.split('.')[0], '%Y-%m-%d %H:%M:%S')
                else:
                    # New user/device/modality - validate all available data
                    print(f"[Thread]       No previous validation found, checking all available data", flush=True)
                    start_time = None  # Will validate from beginning
                
                try:
                    if measure_name == 'label_data':
                        modality_result = validate_postgres_label_data(config["db"], device_id, start_time, None)
                    else:
                        modality_result = validate_timestream_data(
                            ts_query_client, config, device_id, measure_name, start_time, None
                        )
                    
                    device_result['modalities'][measure_name] = modality_result
                    if modality_result.get('has_data'):
                        print(f"[Thread]       ✓ {measure_name}: {modality_result.get('total_rows', 0)} rows", flush=True)
                    else:
                        print(f"[Thread]       - {measure_name}: No data", flush=True)
                except Exception as e:
                    import traceback
                    print(f"[Thread]       ✗ Error validating {measure_name} for device {device_id}: {e}", flush=True)
                    print(f"[Thread]       Traceback: {traceback.format_exc()}", flush=True)
                    device_result['modalities'][measure_name] = {
                        'has_data': False,
                        'error': str(e)
                    }
            
            user_result['devices'].append(device_result)
        
        # Save this user's result immediately
        try:
            save_user_result(results_file, user_result, run_timestamp)
            print(f"[Thread] ✓ Completed validation for user {email}", flush=True)
        except Exception as save_error:
            print(f"[Thread] ✗ Failed to save results for user {email}: {save_error}", flush=True)
            import traceback
            traceback.print_exc()
            # Still return the result even if save failed
            return user_result
        
        return user_result
        
    except Exception as e:
        print(f"[Thread] Error validating user {email}: {e}")
        user_result['error'] = str(e)
        # Still save the error result
        save_user_result(results_file, user_result, run_timestamp)
        return user_result

def run_validation(days_back=1, max_workers=4):
    """Run validation for all users and devices in parallel
    
    For each user/device/modality:
    - If not in history: validate all available data
    - If in history: validate from last checked date to now
    
    Args:
        days_back: Ignored (kept for compatibility, uses history-based approach)
        max_workers: Number of parallel workers (default: 4)
    """
    print(f"Starting data validation at {datetime.now()}")
    print(f"Using {max_workers} parallel workers")
    
    config = load_all_config()
    
    # Validate configurations
    if not all(config["db"].values()):
        raise ValueError("Database configuration is not set")
    
    if not config["timestream"]["database"] or not config["timestream"]["table"]:
        raise ValueError("Timestream configuration is not set")
    
    # Load validation history
    history = load_validation_history()
    print(f"Loaded validation history for {len(history)} users")
    
    # Get all users with devices
    print("Fetching users and devices...")
    users_with_devices = get_all_users_with_devices(config["db"])
    print(f"Found {len(users_with_devices)} users with devices")
    
    # Get run timestamp
    run_timestamp = datetime.now().isoformat()
    
    # Load existing results to preserve them
    existing_results = {}
    if os.path.exists(VALIDATION_RESULTS_FILE):
        try:
            with open(VALIDATION_RESULTS_FILE, 'r') as f:
                existing_results = json.load(f)
            print(f"Loaded existing results for {len(existing_results.get('users', []))} users")
        except Exception as e:
            print(f"Warning: Could not load existing results: {e}")
    
    # Initialize results structure (will be updated incrementally)
    results = {
        'last_run': run_timestamp,
        'validation_strategy': 'incremental',
        'parallel_workers': max_workers,
        'users': existing_results.get('users', [])  # Start with existing users
    }
    
    # Get list of user IDs already in results
    existing_user_ids = {u.get('user_id') for u in results['users']}
    
    # Process users in parallel
    user_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all user validation tasks
        future_to_user = {
            executor.submit(validate_single_user, user_info, config, history, run_timestamp, VALIDATION_RESULTS_FILE): user_info
            for user_info in users_with_devices
        }
        
        # Collect results as they complete (results are already saved per-user)
        completed = 0
        for future in as_completed(future_to_user):
            user_info = future_to_user[future]
            try:
                user_result = future.result()
                user_results.append(user_result)
                completed += 1
                print(f"Progress: {completed}/{len(users_with_devices)} users completed")
            except Exception as e:
                print(f"Error validating user {user_info['email']}: {e}")
                # Add error result and save it
                error_result = {
                    'user_id': user_info['user_id'],
                    'email': user_info['email'],
                    'type': user_info.get('type'),
                    'devices': [],
                    'error': str(e)
                }
                user_results.append(error_result)
                save_user_result(VALIDATION_RESULTS_FILE, error_result, run_timestamp)
    
    # Final results structure (for return value and history update)
    # Note: Results file already has all users saved incrementally
    results['users'] = user_results
    
    # Update final metadata in results file
    try:
        with open(VALIDATION_RESULTS_FILE, 'r') as f:
            final_results = json.load(f)
        final_results['last_run'] = run_timestamp
        final_results['parallel_workers'] = max_workers
        final_results['validation_strategy'] = 'incremental'
        with open(VALIDATION_RESULTS_FILE, 'w') as f:
            json.dump(final_results, f, indent=2, default=str)
    except Exception as e:
        print(f"Warning: Could not update final metadata: {e}")
    
    # Save to history (accumulate date ranges)
    save_validation_history(results)
    
    print(f"\nValidation complete! Results saved incrementally to {VALIDATION_RESULTS_FILE}")
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
    parser.add_argument('--days-back', type=int, default=1, help='Number of days back to check (ignored, uses history-based approach)')
    parser.add_argument('--max-workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    
    args = parser.parse_args()
    
    try:
        results = run_validation(days_back=args.days_back, max_workers=args.max_workers)
        print(f"\nValidation completed successfully!")
        print(f"Total users checked: {len(results['users'])}")
        print(f"Used {results.get('parallel_workers', args.max_workers)} parallel workers")
    except Exception as e:
        print(f"Validation failed: {e}")
        exit(1)


