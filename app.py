from flask import Flask, request, send_file, jsonify, render_template
import os
import tempfile
import json
import threading
import atexit
from threading import Lock
from export import (
    export_sensor_data_to_csv, load_all_config, get_db_connection, MEASURE_DEFINITIONS,
    get_timestream_date_range, get_postgres_label_date_range
)
import psycopg2
import boto3
from botocore.config import Config
from datetime import datetime
from validate_data import load_validation_results, run_validation, load_validation_history
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Initialize scheduler
scheduler = BackgroundScheduler()
scheduler.start()

# Default validation settings
VALIDATION_DAYS_BACK = int(os.environ.get('VALIDATION_DAYS_BACK', '1'))
VALIDATION_RUN_ON_STARTUP = os.environ.get('VALIDATION_RUN_ON_STARTUP', 'false').lower() == 'true'

# Track validation status (thread-safe)
validation_status = {
    'is_running': False,
    'start_time': None,
    'last_completed': None
}
validation_lock = Lock()

def run_validation_job():
    """Background job to run data validation"""
    global validation_status
    with validation_lock:
        validation_status['is_running'] = True
        validation_status['start_time'] = datetime.now().isoformat()
    
    try:
        print(f"[Validation Job] Starting scheduled validation at {datetime.now()}")
        run_validation(days_back=VALIDATION_DAYS_BACK)
        with validation_lock:
            validation_status['last_completed'] = datetime.now().isoformat()
        print(f"[Validation Job] Validation completed successfully at {datetime.now()}")
    except Exception as e:
        print(f"[Validation Job] Validation failed: {e}")
    finally:
        with validation_lock:
            validation_status['is_running'] = False
            validation_status['start_time'] = None

# Schedule validation to run daily at 2 AM
scheduler.add_job(
    func=run_validation_job,
    trigger='cron',
    hour=2,
    minute=0,
    id='daily_validation',
    name='Daily Data Validation',
    replace_existing=True
)

# Optionally run validation on startup if configured
if VALIDATION_RUN_ON_STARTUP:
    print("[Validation] Running validation on startup...")
    # Run in a separate thread to not block Flask startup
    threading.Thread(target=run_validation_job, daemon=True).start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

@app.route('/export', methods=['POST', 'GET'])
def export_data():
    """
    Export sensor data to CSV based on device_id, start_time, end_time, and measure_name.
    
    Accepts parameters via:
    - POST: JSON body or form data
    - GET: Query parameters
    
    Parameters:
    - device_id (required): The ID of the device
    - start_time (optional): Start timestamp (YYYY-MM-DD HH:MM:SS)
    - end_time (optional): End timestamp (YYYY-MM-DD HH:MM:SS)
    - measure_name (required): The measure name (e.g., 'motion_data', 'heart_rate_data', 'label_data')
    - output_file (optional): Custom output filename. If not provided, auto-generated.
    """
    try:
        # Get parameters from request
        if request.method == 'POST':
            if request.is_json:
                data = request.get_json()
            else:
                data = request.form.to_dict()
        else:  # GET
            data = request.args.to_dict()
        
        # Extract required parameters
        device_id = data.get('device_id')
        measure_name = data.get('measure_name')
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        output_file = data.get('output_file')
        
        # Validate required parameters
        if not device_id:
            return jsonify({'error': 'device_id is required'}), 400
        if not measure_name:
            return jsonify({'error': 'measure_name is required'}), 400
        
        # Generate output filename if not provided
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"{device_id}-{measure_name}-{timestamp}.csv"
        
        # Ensure output file is in a temporary directory or current directory
        # Using tempfile for better security and cleanup
        temp_dir = tempfile.gettempdir()
        output_path = os.path.join(temp_dir, output_file)
        
        # Call the export function
        export_sensor_data_to_csv(
            device_id=device_id,
            output_file=output_path,
            measure_name=measure_name,
            start_time=start_time,
            end_time=end_time
        )
        
        # Check if file was created
        if not os.path.exists(output_path):
            return jsonify({
                'error': 'Export completed but no data was found for the specified parameters',
                'message': 'No data rows were written to the CSV file'
            }), 404
        
        # Return the CSV file as a download
        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_file,
            mimetype='text/csv'
        )
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500

def get_user_devices(db_config, user_id):
    """Get all devices/watches associated with a user"""
    devices = []
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT w.id, w.hardware_id, w.created_at
                    FROM watches w
                    JOIN users_watches uw ON w.id = uw.watch_id
                    WHERE uw.user_id = %s
                    ORDER BY w.hardware_id ASC;
                """, (user_id,))
                device_results = cursor.fetchall()
                
                for device_row in device_results:
                    devices.append({
                        'watch_id': device_row[0],
                        'hardware_id': device_row[1],
                        'created_at': str(device_row[2]) if device_row[2] else None
                    })
    except psycopg2.Error as e:
        raise Exception(f'Database error: {str(e)}')
    return devices

@app.route('/users', methods=['GET'])
def list_users():
    """List all users from the database"""
    try:
        config = load_all_config()
        
        # Validate DB config
        if not all(config["db"].values()):
            return jsonify({'error': 'Database configuration is not set. Please set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD environment variables.'}), 500
        
        users = []
        try:
            with get_db_connection(config["db"]) as conn:
                with conn.cursor() as cursor:
                    # Query all users
                    cursor.execute("""
                        SELECT id, email, type, created_at
                        FROM users
                        ORDER BY id ASC;
                    """)
                    user_results = cursor.fetchall()
                    
                    # Format results as list of dictionaries
                    for user_row in user_results:
                        users.append({
                            'id': user_row[0],
                            'email': user_row[1],
                            'type': user_row[2],
                            'created_at': str(user_row[3]) if user_row[3] else None
                        })
        except psycopg2.Error as e:
            return jsonify({'error': f'Database error: {str(e)}'}), 500
        
        return jsonify({
            'count': len(users),
            'users': users
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500

@app.route('/api/user/<int:user_id>/devices', methods=['GET'])
def get_devices_for_user(user_id):
    """Get all devices for a specific user"""
    try:
        config = load_all_config()
        
        if not all(config["db"].values()):
            return jsonify({'error': 'Database configuration is not set.'}), 500
        
        devices = get_user_devices(config["db"], user_id)
        return jsonify({
            'count': len(devices),
            'devices': devices
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/date-range', methods=['GET'])
def get_date_range():
    """Get the available date range for a device and measure type"""
    try:
        device_id = request.args.get('device_id')
        measure_name = request.args.get('measure_name')
        
        if not device_id or not measure_name:
            return jsonify({'error': 'device_id and measure_name are required'}), 400
        
        if measure_name not in MEASURE_DEFINITIONS:
            return jsonify({'error': f'Invalid measure_name: {measure_name}'}), 400
        
        config = load_all_config()
        min_time = None
        max_time = None
        
        if measure_name == "label_data":
            # Query PostgreSQL for label data date range
            if not all(config["db"].values()):
                return jsonify({'error': 'Database configuration is not set.'}), 500
            
            min_time, max_time = get_postgres_label_date_range(config["db"], device_id)
        else:
            # Query Timestream for sensor data date range
            if not config["timestream"]["database"] or not config["timestream"]["table"]:
                return jsonify({'error': 'Timestream configuration is not set.'}), 500
            
            ts_query_client = boto3.client('timestream-query', config=Config(read_timeout=30, retries={'max_attempts': 5}))
            min_time, max_time = get_timestream_date_range(ts_query_client, config, device_id, measure_name)
        
        if min_time is None or max_time is None:
            return jsonify({
                'error': 'No data found for the specified device and measure type',
                'min_time': None,
                'max_time': None
            }), 404
        
        # Format dates for datetime-local input (YYYY-MM-DDTHH:MM)
        # Normalize different datetime formats
        def normalize_datetime(dt_str):
            """Convert various datetime formats to YYYY-MM-DDTHH:MM format"""
            if not dt_str:
                return None
            
            # Handle ISO format with T separator (from Timestream)
            if 'T' in dt_str:
                dt_str = dt_str.replace('T', ' ')
            
            # Parse and reformat
            try:
                # Try parsing different formats
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']:
                    try:
                        dt = datetime.strptime(dt_str.split('.')[0].split('+')[0].strip(), fmt)
                        return dt.strftime('%Y-%m-%dT%H:%M')
                    except ValueError:
                        continue
                # If all parsing fails, try to extract YYYY-MM-DDTHH:MM
                if len(dt_str) >= 16:
                    return dt_str[:16].replace(' ', 'T')
                return dt_str
            except Exception:
                return dt_str[:16].replace(' ', 'T') if len(dt_str) >= 16 else dt_str
        
        min_formatted = normalize_datetime(min_time)
        max_formatted = normalize_datetime(max_time)
        
        return jsonify({
            'min_time': min_formatted,
            'max_time': max_formatted,
            'min_time_full': min_time,
            'max_time_full': max_time
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500

@app.route('/api/validation-results', methods=['GET'])
def get_validation_results():
    """Get validation results from the last run"""
    try:
        # Check if validation is currently running (thread-safe)
        with validation_lock:
            is_running = validation_status['is_running']
            start_time = validation_status.get('start_time')
        
        if is_running:
            return jsonify({
                'status': 'running',
                'message': 'Validation is currently in progress',
                'start_time': start_time,
                'is_running': True
            }), 202  # Accepted - processing
        
        results = load_validation_results()
        if results is None:
            return jsonify({
                'error': 'No validation results found. Validation will run automatically.',
                'last_run': None,
                'is_running': False
            }), 404
        
        # Add scheduler info
        jobs = scheduler.get_jobs()
        next_run = None
        for job in jobs:
            if job.id == 'daily_validation':
                next_run = job.next_run_time.isoformat() if job.next_run_time else None
                break
        
        results['scheduler'] = {
            'enabled': True,
            'next_run': next_run,
            'days_back': VALIDATION_DAYS_BACK
        }
        
        # Add validation history
        history = load_validation_history()
        results['history'] = history
        
        # Add status info (thread-safe)
        with validation_lock:
            results['is_running'] = False
            results['last_completed'] = validation_status.get('last_completed')
        
        return jsonify(results), 200
        
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500

@app.route('/api/validation/trigger', methods=['POST'])
def trigger_validation():
    """Manually trigger a validation run"""
    try:
        # Run validation in background thread
        threading.Thread(target=run_validation_job, daemon=True).start()
        return jsonify({
            'message': 'Validation job started in background',
            'status': 'running'
        }), 202
    except Exception as e:
        return jsonify({'error': f'Failed to start validation: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/admin', methods=['GET'])
def admin_page():
    """Admin page for exporting user data"""
    return render_template('admin.html')

@app.route('/', methods=['GET'])
def index():
    """API documentation endpoint"""
    return jsonify({
        'message': 'LemurDX Data Export API',
        'endpoints': {
            '/admin': {
                'methods': ['GET'],
                'description': 'Admin page for exporting user data'
            },
            '/export': {
                'methods': ['GET', 'POST'],
                'description': 'Export sensor data to CSV',
                'parameters': {
                    'device_id': 'required - The ID of the device',
                    'measure_name': 'required - The measure name (e.g., motion_data, heart_rate_data, location_data, label_data)',
                    'start_time': 'optional - Start timestamp (YYYY-MM-DD HH:MM:SS)',
                    'end_time': 'optional - End timestamp (YYYY-MM-DD HH:MM:SS)',
                    'output_file': 'optional - Custom output filename'
                },
                'example': {
                    'url': '/export?device_id=9001&measure_name=motion_data&start_time=2025-08-26 00:00:00&end_time=2025-08-26 23:59:59',
                    'post_json': {
                        'device_id': '9001',
                        'measure_name': 'motion_data',
                        'start_time': '2025-08-26 00:00:00',
                        'end_time': '2025-08-26 23:59:59'
                    }
                }
            },
            '/users': {
                'methods': ['GET'],
                'description': 'List all users from the database'
            },
            '/api/user/<user_id>/devices': {
                'methods': ['GET'],
                'description': 'Get all devices for a specific user'
            },
            '/health': {
                'methods': ['GET'],
                'description': 'Health check endpoint'
            }
        }
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

