import argparse
import os
import csv
import boto3
import psycopg2
import textwrap
from datetime import datetime, timedelta
from botocore.config import Config
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
def load_all_config():
    """Loads all necessary configurations from environment variables."""
    return {
        "db": {
            "host": os.environ.get('DB_HOST'),
            "dbname": os.environ.get('DB_NAME'),
            "user": os.environ.get('DB_USER'),
            "password": os.environ.get('DB_PASSWORD'),
            "port": os.environ.get('DB_PORT', '5432'),
        },
        "timestream": {
            "database": os.environ.get('TIMESTREAM_DATABASE'),
            "table": os.environ.get('TIMESTREAM_TABLE'),
        }
    }

# --- Database Connection Context Manager ---
@contextmanager
def get_db_connection(db_config):
    """Provides a database connection using a context manager for automatic cleanup."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        yield conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

# --- Timestream Helper Functions ---
def _paginate_timestream_query(client, query):
    """A generic helper to handle pagination for any Timestream query."""
    all_rows = []
    next_token = None
    while True:
        kwargs = {'QueryString': query}
        if next_token:
            kwargs['NextToken'] = next_token

        response = client.query(**kwargs)
        all_rows.extend(response.get('Rows', []))

        next_token = response.get('NextToken')
        if not next_token:
            break
    response['Rows'] = all_rows
    return response


def parse_timestream_response(response):
    """Parses a Timestream query response into a list of dictionaries."""
    column_info = response.get('ColumnInfo', [])
    rows = response.get('Rows', [])

    parsed_rows = []
    for row in rows:
        data_point = {}
        for i, cell in enumerate(row.get('Data', [])):
            if not cell.get('NullValue'):
                column_name = column_info[i]['Name']
                data_point[column_name] = cell.get('ScalarValue')
        if data_point:
            parsed_rows.append(data_point)

    return parsed_rows

# --- Measure Definitions ---
MEASURE_DEFINITIONS = {
    "motion_data": [
        "acceleration_x", "acceleration_y", "acceleration_z",
        "gravity_x", "gravity_y", "gravity_z",
        "rotation_x", "rotation_y", "rotation_z",
        "mag_field_x", "mag_field_y", "mag_field_z",
        "roll", "pitch", "yaw",
        "quaternion_x", "quaternion_y", "quaternion_z", "quaternion_w"
    ],
    "heart_rate_data": [
        "heart_rate"
    ],
    "location_data": [
        "latitude", "longitude", "altitude"
    ],
    "label_data": [
        "device_id", "uid", "created_at", "completed_at", "enjoyment", "note", "training_activity_name"
    ]
    # Add other measure types here as needed
}

# --- Query Functions ---
def query_timestream_measure_data(ts_query_client, config, device_id, measure_name, sub_measure_names, start_time, end_time):
    # Construct the SELECT clause dynamically
    select_measures_sql = ", ".join([f"{name}" for name in sub_measure_names])

    # Construct the WHERE clause for time range
    time_clause = ""
    if start_time and end_time:
        time_clause = f"AND time BETWEEN TIMESTAMP '{start_time}' AND TIMESTAMP '{end_time}'"
    elif start_time:
        time_clause = f"AND time >= TIMESTAMP '{start_time}'"
    elif end_time:
        time_clause = f"AND time <= TIMESTAMP '{end_time}'"
    else:
        # Default to last 7 days if no time range is specified
        default_start_time = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        time_clause = f"AND time >= TIMESTAMP '{default_start_time}'"

    query = f"""
        SELECT time, {select_measures_sql}
        FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
        WHERE measure_name = '{measure_name}'
          AND device_id = '{device_id}'
          {time_clause}
        ORDER BY time ASC
    """

    print(f"Executing Timestream query for device {device_id}, measure '{measure_name}'...")
    print("--- Generated Timestream Query ---")
    print(query)
    print("----------------------------------")
    response = _paginate_timestream_query(ts_query_client, query)
    data_rows = parse_timestream_response(response)

    if not data_rows:
        print(f"No data found for device {device_id} and measure '{measure_name}' in the specified time range.")
    else:
        print(f"--- Sample Data Rows (first 5) for measure '{measure_name}' ---")
        for i, row in enumerate(data_rows[:5]):
            print(f"Row {i}: {row}")
        print("----------------------------------")
    return data_rows

# --- PostgreSQL Helper Functions ---
def _get_watch_id_by_hardware_id(cursor, device_id):
    watch_query = textwrap.dedent("""
        SELECT id FROM watches WHERE hardware_id = %s;
    """)
    cursor.execute(watch_query, (device_id,))
    watch_id_result = cursor.fetchone()
    if not watch_id_result:
        print(f"No watch found for device_id: {device_id}")
        return None
    return watch_id_result[0]

def _get_participants_by_watch_id(cursor, watch_id):
    users_query = textwrap.dedent("""
        SELECT u.id, u.email
        FROM users u
        JOIN users_watches uw ON u.id = uw.user_id
        WHERE uw.watch_id = %s AND u.type = 'Participant';
    """)
    cursor.execute(users_query, (watch_id,))
    participant_results = cursor.fetchall()

    if not participant_results:
        print(f"No participants found for watch_id: {watch_id}")
        return []
    
    print("--- Found Participants ---")
    for p_id, p_email in participant_results:
        print(f"  ID: {p_id}, Email: {p_email}")
    print("--------------------------")
    return participant_results

def _get_training_labels_for_participants(cursor, participant_ids, sub_measure_names):
    label_cols_from_db = [col for col in sub_measure_names if col not in ["device_id", "uid", "training_activity_name"]]
    select_label_cols = ", ".join([f"tl.{col}" for col in label_cols_from_db])
    select_label_cols += ", ta.name AS training_activity_name"

    labels_query = textwrap.dedent(f"""
        SELECT {select_label_cols}, tl.participant_id
        FROM training_labels tl
        LEFT JOIN training_activities ta ON tl.training_activity_id = ta.id
        WHERE tl.participant_id IN %s;
    """)
    cursor.execute(labels_query, (tuple(participant_ids),))
    
    label_col_names = [desc[0] for desc in cursor.description]
    label_rows_tuples = cursor.fetchall()
    return label_rows_tuples, label_col_names

def _combine_and_format_label_results(device_id, participant_results, label_rows_tuples, label_col_names, sub_measure_names):
    data_rows = []
    participant_map = {p[0]: p[1] for p in participant_results} # Map participant_id to email

    for label_row_tuple in label_rows_tuples:
        label_data_dict = dict(zip(label_col_names, label_row_tuple))
        
        participant_id_for_label = label_data_dict.pop('participant_id')
        participant_email = participant_map.get(participant_id_for_label)

        formatted_uid = participant_email.split('@')[0] if participant_email and '@' in participant_email else participant_email

        final_row = {
            'device_id': device_id,
            'uid': formatted_uid
        }
        # Add other requested columns from label_data_dict
        for col in sub_measure_names:
            if col not in ["device_id", "uid"]:
                final_row[col] = label_data_dict.get(col)
        
        data_rows.append(final_row)
    return data_rows

def get_timestream_date_range(ts_query_client, config, device_id, measure_name):
    """Get the min and max date range for a device and measure in Timestream"""
    query = f"""
        SELECT 
            MIN(time) as min_time,
            MAX(time) as max_time
        FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
        WHERE measure_name = '{measure_name}'
          AND device_id = '{device_id}'
    """
    
    try:
        response = _paginate_timestream_query(ts_query_client, query)
        rows = parse_timestream_response(response)
        
        if rows and len(rows) > 0:
            min_time = rows[0].get('min_time')
            max_time = rows[0].get('max_time')
            return min_time, max_time
    except Exception as e:
        print(f"Error getting Timestream date range: {e}")
    
    return None, None

def get_postgres_label_date_range(db_config, device_id):
    """Get the min and max date range for label data in PostgreSQL"""
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                watch_id = _get_watch_id_by_hardware_id(cursor, device_id)
                if watch_id is None:
                    return None, None
                
                participant_results = _get_participants_by_watch_id(cursor, watch_id)
                if not participant_results:
                    return None, None
                
                participant_ids = [p[0] for p in participant_results]
                
                # Query min and max dates from training_labels
                cursor.execute("""
                    SELECT 
                        MIN(created_at) as min_time,
                        MAX(COALESCE(completed_at, created_at)) as max_time
                    FROM training_labels
                    WHERE participant_id IN %s;
                """, (tuple(participant_ids),))
                
                result = cursor.fetchone()
                if result and result[0] and result[1]:
                    return str(result[0]), str(result[1])
    except psycopg2.Error as e:
        print(f"Error getting PostgreSQL date range: {e}")
    
    return None, None

def query_postgres_label_data(db_config, device_id, sub_measure_names):
    print(f"Executing PostgreSQL query for device {device_id}, measure group 'label_data'...")
    data_rows = []
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                watch_id = _get_watch_id_by_hardware_id(cursor, device_id)
                if watch_id is None:
                    return []

                participant_results = _get_participants_by_watch_id(cursor, watch_id)
                if not participant_results:
                    return []
                participant_ids = [p[0] for p in participant_results]

                label_rows_tuples, label_col_names = _get_training_labels_for_participants(cursor, participant_ids, sub_measure_names)
                if not label_rows_tuples:
                    print(f"No label data found for device {device_id}.")
                    return []

                data_rows = _combine_and_format_label_results(device_id, participant_results, label_rows_tuples, label_col_names, sub_measure_names)

        if not data_rows:
            print(f"No label data found for device {device_id}.") # This print might be redundant if _combine_and_format_label_results returns empty
        else:
            print(f"--- Sample Data Rows (first 5) for measure group 'label_data' ---")
            for i, row in enumerate(data_rows[:5]):
                print(f"Row {i}: {row}")
            print("----------------------------------")

    except psycopg2.Error as e:
        print(f"PostgreSQL query error: {e}")
        raise
    return data_rows

# --- Main Export Logic ---
def export_sensor_data_to_csv(device_id, output_file, measure_name, start_time=None, end_time=None):
    config = load_all_config()

    # Validate Timestream config if measure_name is not label_data
    if measure_name != "label_data" and (not config["timestream"]["database"] or not config["timestream"]["table"]):
        raise ValueError("Error: TIMESTREAM_DATABASE or TIMESTREAM_TABLE environment variables are not set for Timestream query.")

    # Validate DB config if measure_name is label_data
    if measure_name == "label_data" and not all(config["db"].values()):
        raise ValueError("Error: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD environment variables are not set for PostgreSQL query.")

    if measure_name not in MEASURE_DEFINITIONS:
        raise ValueError(f"Unknown measure_name: '{measure_name}'. Supported measures are: {', '.join(MEASURE_DEFINITIONS.keys())}")

    sub_measure_names = MEASURE_DEFINITIONS[measure_name]
    data_rows = []

    if measure_name == "label_data":
        data_rows = query_postgres_label_data(config["db"], device_id, sub_measure_names)
    else:
        ts_query_client = boto3.client('timestream-query', config=Config(read_timeout=30, retries={'max_attempts': 5}))
        data_rows = query_timestream_measure_data(ts_query_client, config, device_id, measure_name, sub_measure_names, start_time, end_time)

    if not data_rows:
        return # Already printed "No data found" inside query functions

    csv_header = ['time'] + sub_measure_names if measure_name != "label_data" else sub_measure_names

    print(f"Writing {len(data_rows)} rows to {output_file}...")
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_header)
        writer.writeheader()
        writer.writerows(data_rows)

    print("Export complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export sensor data for a given device_id from AWS Timestream or PostgreSQL to a CSV file.")
    parser.add_argument('--device_id', required=True, help="The ID of the device to export data for.")
    parser.add_argument('--output_file', required=True, help="The path to the output CSV file.")
    parser.add_argument('--measure_name', required=True, help="The name of the measure to export (e.g., 'motion_data', 'heart_rate_data', 'label_data').")
    parser.add_argument('--start_time', help="Optional: Start timestamp for data (YYYY-MM-DD HH:MM:SS). Only applicable for Timestream data.")
    parser.add_argument('--end_time', help="Optional: End timestamp for data (YYYY-MM-DD HH:MM:SS). Only applicable for Timestream data.")

    args = parser.parse_args()

    export_sensor_data_to_csv(
        device_id=args.device_id,
        output_file=args.output_file,
        measure_name=args.measure_name,
        start_time=args.start_time,
        end_time=args.end_time
    )