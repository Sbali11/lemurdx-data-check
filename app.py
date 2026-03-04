from flask import Flask, request, send_file, jsonify, render_template, Response
from functools import wraps
import os
import tempfile
import threading
import csv
import sqlite3
from io import StringIO
from export import export_sensor_data_to_csv, load_all_config, get_db_connection, get_timestream_date_range, get_postgres_label_date_range
from validate_data import get_all_users_with_devices, load_validation_results, run_validation
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit
import time

app = Flask(__name__)

# Simple TTL cache for daily availability queries (avoids repeated expensive Timestream scans)
_daily_availability_cache = {}
_CACHE_TTL_SECONDS = 3600  # 1 hour

# SQLite database for app-level settings (custom enrollment periods, etc.)
SETTINGS_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.db')

def init_settings_db():
    """Initialize the settings SQLite database"""
    conn = sqlite3.connect(SETTINGS_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_enrollment_periods (
            user_id INTEGER NOT NULL,
            device_id TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, device_id)
        )
    """)
    conn.commit()
    conn.close()

def get_settings_db():
    """Get a connection to the settings database"""
    conn = sqlite3.connect(SETTINGS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

init_settings_db()

# Admin credentials (can be overridden via environment variables)
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD')  # Change this in production!

# Track if validation is running
_validation_lock = threading.Lock()
_validation_running = False

# Scheduler configuration
VALIDATION_SCHEDULE_HOUR = int(os.environ.get('VALIDATION_SCHEDULE_HOUR', '2'))  # Default: 2 AM
VALIDATION_SCHEDULE_MINUTE = int(os.environ.get('VALIDATION_SCHEDULE_MINUTE', '0'))  # Default: 0 minutes
VALIDATION_DAYS_BACK = int(os.environ.get('VALIDATION_DAYS_BACK', '7'))  # Default: 7 days
VALIDATION_MAX_WORKERS = int(os.environ.get('VALIDATION_MAX_WORKERS', '4'))  # Default: 4 workers
VALIDATION_ENABLED = os.environ.get('VALIDATION_ENABLED', 'true').lower() == 'true'  # Default: enabled

# Initialize scheduler
scheduler = BackgroundScheduler(daemon=True)

def scheduled_validation():
    """Run scheduled validation (called by scheduler)"""
    global _validation_running

    print(f"[Scheduler] Starting scheduled validation at {datetime.now()}")

    with _validation_lock:
        if _validation_running:
            print("[Scheduler] Validation already running, skipping scheduled run")
            return
        _validation_running = True

    try:
        run_validation(days_back=VALIDATION_DAYS_BACK, max_workers=VALIDATION_MAX_WORKERS)
        print(f"[Scheduler] Scheduled validation completed successfully at {datetime.now()}")
    except Exception as e:
        print(f"[Scheduler] Scheduled validation failed: {e}")
    finally:
        with _validation_lock:
            _validation_running = False

# Add scheduled job if validation is enabled
if VALIDATION_ENABLED:
    trigger = CronTrigger(hour=VALIDATION_SCHEDULE_HOUR, minute=VALIDATION_SCHEDULE_MINUTE)
    scheduler.add_job(
        func=scheduled_validation,
        trigger=trigger,
        id='daily_validation',
        name='Daily Data Validation',
        replace_existing=True
    )
    print(f"[Scheduler] Validation scheduled daily at {VALIDATION_SCHEDULE_HOUR:02d}:{VALIDATION_SCHEDULE_MINUTE:02d}")
else:
    print("[Scheduler] Automatic validation is disabled")

# Start the scheduler
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

def check_auth(username, password):
    """Check if username and password are correct"""
    return username == ADMIN_USERNAME and password == ADMIN_PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth"""
    # Check if this is an API request (JSON expected)
    if request.path.startswith('/api/') or request.accept_mimetypes.accept_json:
        return jsonify({
            'error': 'Authentication required',
            'message': 'You must login with proper credentials'
        }), 401, {'WWW-Authenticate': 'Basic realm="Login Required"'}
    else:
        return Response(
            'Could not verify your access level for that URL.\n'
            'You have to login with proper credentials', 401,
            {'WWW-Authenticate': 'Basic realm="Login Required"'}
        )

def requires_auth(f):
    """Decorator to require HTTP Basic Auth"""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

@app.route('/export', methods=['POST', 'GET'])
@requires_auth
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

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/admin', methods=['GET'])
@requires_auth
def admin_page():
    """Admin index page (password protected)"""
    return render_template('admin.html')

@app.route('/admin/export', methods=['GET'])
@requires_auth
def admin_export():
    """Admin page for exporting user data (password protected)"""
    return render_template('export.html')

@app.route('/admin/validation', methods=['GET'])
@requires_auth
def admin_validation():
    """Admin page for data validation (password protected)"""
    return render_template('validation.html')

@app.route('/admin/error-details', methods=['GET'])
@requires_auth
def admin_error_details():
    """Admin page for sensor coverage error details (password protected)"""
    return render_template('error_details.html')

@app.route('/admin/participant-download', methods=['GET'])
@requires_auth
def admin_participant_download():
    """Admin page for downloading participant data by sensor (password protected)"""
    return render_template('participant_download.html')

@app.route('/users', methods=['GET'])
@requires_auth
def get_users():
    """Get all users with devices"""
    try:
        config = load_all_config()
        users_with_devices = get_all_users_with_devices(config["db"])
        
        # Format for frontend
        users = []
        for user_info in users_with_devices:
            users.append({
                'id': user_info['user_id'],
                'email': user_info['email'],
                'type': user_info['type']
            })
        
        return jsonify({'users': users}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<int:user_id>/devices', methods=['GET'])
@requires_auth
def get_user_devices(user_id):
    """Get devices for a specific user"""
    try:
        config = load_all_config()
        users_with_devices = get_all_users_with_devices(config["db"])
        
        # Find user
        user_info = None
        for u in users_with_devices:
            if u['user_id'] == user_id:
                user_info = u
                break
        
        if not user_info:
            return jsonify({'error': 'User not found'}), 404
        
        # Format devices
        devices = []
        for device_id in user_info['devices']:
            devices.append({
                'hardware_id': device_id
            })
        
        return jsonify({'devices': devices}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/date-range', methods=['GET'])
@requires_auth
def get_date_range():
    """Get date range for a device and measure"""
    try:
        device_id = request.args.get('device_id')
        measure_name = request.args.get('measure_name')
        
        if not device_id or not measure_name:
            return jsonify({'error': 'device_id and measure_name are required'}), 400
        
        config = load_all_config()
        
        # Get date range based on measure type
        if measure_name == 'label_data':
            min_time, max_time = get_postgres_label_date_range(config["db"], device_id)
        else:
            # Create Timestream client
            import boto3
            from botocore.config import Config
            ts_config = Config(
                region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                retries={'max_attempts': 3}
            )
            ts_query_client = boto3.client('timestream-query', config=ts_config)
            min_time, max_time = get_timestream_date_range(ts_query_client, config, device_id, measure_name)
        
        if min_time is None or max_time is None:
            return jsonify({'error': 'No data found for this device and measure type'}), 404
        
        # Format for datetime-local input (YYYY-MM-DDTHH:MM)
        min_time_dt = datetime.fromisoformat(min_time.replace('Z', '+00:00') if 'Z' in min_time else min_time)
        max_time_dt = datetime.fromisoformat(max_time.replace('Z', '+00:00') if 'Z' in max_time else max_time)
        
        return jsonify({
            'min_time': min_time_dt.strftime('%Y-%m-%dT%H:%M'),
            'max_time': max_time_dt.strftime('%Y-%m-%dT%H:%M'),
            'min_time_full': min_time,
            'max_time_full': max_time
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/validation-results', methods=['GET'])
@requires_auth
def get_validation_results():
    """Get validation results"""
    try:
        results = load_validation_results()

        # Get scheduler information
        scheduler_info = {
            'enabled': VALIDATION_ENABLED,
            'days_back': VALIDATION_DAYS_BACK,
            'max_workers': VALIDATION_MAX_WORKERS
        }

        if VALIDATION_ENABLED:
            # Get next run time
            job = scheduler.get_job('daily_validation')
            if job:
                next_run = job.next_run_time
                if next_run:
                    scheduler_info['next_run'] = next_run.isoformat()
                scheduler_info['schedule'] = f"Daily at {VALIDATION_SCHEDULE_HOUR:02d}:{VALIDATION_SCHEDULE_MINUTE:02d}"

        if results is None:
            # Check if validation is running
            with _validation_lock:
                is_running = _validation_running

            if is_running:
                return jsonify({
                    'is_running': True,
                    'status': 'running',
                    'users': [],
                    'message': 'Validation is running...',
                    'scheduler': scheduler_info
                }), 202

            return jsonify({
                'message': 'No validation results found. Click "Run Now" to start validation.',
                'last_run': None,
                'scheduler': scheduler_info
            }), 404

        # Check if validation is still running
        with _validation_lock:
            is_running = _validation_running

        # Add scheduler info to results
        results['scheduler'] = scheduler_info

        if is_running:
            return jsonify({
                'is_running': True,
                'status': 'running',
                **results
            }), 202

        return jsonify(results), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/validation-results/csv', methods=['GET'])
@requires_auth
def download_validation_csv():
    """Download validation results as CSV"""
    try:
        results = load_validation_results()
        
        if results is None:
            return jsonify({'error': 'No validation results found'}), 404
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'User ID', 'Email', 'User Type', 'Device ID', 'Modality',
            'Has Data', 'Total Rows', 'Missing Values Count', 'Format Errors',
            'Format Valid', 'Gap Count', 'Sampling Frequency'
        ])
        
        # Write data
        for user in results.get('users', []):
            for device in user.get('devices', []):
                for modality, mod_result in (device.get('modalities') or {}).items():
                    missing_count = sum(
                        (mv.get('count', 0) for mv in (mod_result.get('missing_values') or {}).values()),
                        0
                    )
                    format_errors = len(mod_result.get('format_errors', []))
                    gap_count = (mod_result.get('sampling_stats') or {}).get('gap_count', 0)
                    sampling_freq = (mod_result.get('sampling_stats') or {}).get('sampling_frequency', '')
                    
                    writer.writerow([
                        user.get('user_id', ''),
                        user.get('email', ''),
                        user.get('type', ''),
                        device.get('device_id', ''),
                        modality,
                        mod_result.get('has_data', False),
                        mod_result.get('total_rows', 0),
                        missing_count,
                        format_errors,
                        mod_result.get('format_valid', True),
                        gap_count,
                        sampling_freq
                    ])
        
        csv_data = output.getvalue()
        output.close()
        
        # Return as download
        response = Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=validation_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<int:user_id>/device/<device_id>/enrollment', methods=['GET'])
@requires_auth
def get_user_enrollment_period(user_id, device_id):
    """Get the enrollment period for a specific user and device"""
    try:
        config = load_all_config()
        enrollment = get_user_device_enrollment_dates(config["db"], user_id, device_id)

        if not enrollment:
            return jsonify({'error': 'No enrollment data found'}), 404

        return jsonify(enrollment), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_user_device_enrollment_dates(db_config, user_id, device_id):
    """Get the enrollment period for a user with a specific device from label data"""
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                # Get the date range when this user was creating labels with this device
                query = """
                    SELECT
                        MIN(DATE(tl.created_at)) as start_date,
                        MAX(DATE(COALESCE(tl.completed_at, tl.created_at))) as end_date
                    FROM training_labels tl
                    JOIN watches w ON w.id = tl.watch_id
                    WHERE tl.participant_id = %s AND w.hardware_id = %s
                """
                cursor.execute(query, (user_id, device_id))
                result = cursor.fetchone()

                if result and result[0] and result[1]:
                    return {
                        'user_id': user_id,
                        'device_id': device_id,
                        'start_date': result[0].strftime('%Y-%m-%d'),
                        'end_date': result[1].strftime('%Y-%m-%d')
                    }
                return None
    except Exception as e:
        print(f"Error getting user enrollment dates: {e}")
        return None

@app.route('/api/device/<device_id>/sensor/<sensor>/daily-availability', methods=['GET'])
@requires_auth
def get_daily_availability(device_id, sensor):
    """Get daily data availability for a device and sensor, optionally scoped to a user's enrollment period"""
    try:
        config = load_all_config()

        # Get date range from query params
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user_id = request.args.get('user_id')

        # If user_id is provided, get their enrollment period and use it to scope the dates
        if user_id:
            enrollment = get_user_device_enrollment_dates(config["db"], int(user_id), device_id)
            if enrollment:
                # Use enrollment dates to scope the query (intersect with provided dates)
                if start_date:
                    start_date = max(start_date, enrollment['start_date'])
                else:
                    start_date = enrollment['start_date']
                if end_date:
                    end_date = min(end_date, enrollment['end_date'])
                else:
                    end_date = enrollment['end_date']

        if sensor == 'label_data':
            # Query PostgreSQL for label data dates (can filter by user)
            days_with_data = get_label_days_with_data(config["db"], device_id, start_date, end_date, user_id)
        else:
            # Query Timestream for sensor data dates
            days_with_data = get_timestream_days_with_data(config, device_id, sensor, start_date, end_date)

        return jsonify({
            'device_id': device_id,
            'sensor': sensor,
            'days_with_data': days_with_data
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_timestream_days_with_data(config, device_id, measure_name, start_date=None, end_date=None):
    """Query Timestream to get list of dates that have data (with TTL cache)"""
    # Check cache first
    cache_key = (device_id, measure_name, start_date, end_date)
    cached = _daily_availability_cache.get(cache_key)
    if cached and (time.time() - cached['ts']) < _CACHE_TTL_SECONDS:
        return cached['data']

    import boto3
    from botocore.config import Config as BotoConfig

    ts_config = BotoConfig(
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        read_timeout=60,
        retries={'max_attempts': 3}
    )
    ts_query_client = boto3.client('timestream-query', config=ts_config)

    # Build query to get distinct dates with data
    query = f"""
        SELECT DISTINCT DATE(time) as data_date
        FROM "{config['timestream']['database']}"."{config['timestream']['table']}"
        WHERE measure_name = '{measure_name}'
          AND device_id = '{device_id}'
    """

    if start_date:
        query += f" AND time >= TIMESTAMP '{start_date} 00:00:00'"
    if end_date:
        query += f" AND time <= TIMESTAMP '{end_date} 23:59:59'"

    query += " ORDER BY data_date ASC"

    from export import _paginate_timestream_query, parse_timestream_response

    try:
        response = _paginate_timestream_query(ts_query_client, query)
        rows = parse_timestream_response(response)

        # Extract dates as strings
        dates = []
        for row in rows:
            if 'data_date' in row and row['data_date']:
                date_str = str(row['data_date']).split(' ')[0]  # Get just the date part
                dates.append(date_str)

        # Cache the result
        _daily_availability_cache[cache_key] = {'data': dates, 'ts': time.time()}
        return dates
    except Exception as e:
        print(f"Error querying Timestream for daily availability: {e}")
        return []

def get_label_days_with_data(db_config, device_id, start_date=None, end_date=None, user_id=None):
    """Query PostgreSQL to get list of dates that have label data, optionally filtered by user"""
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT DISTINCT DATE(tl.created_at) as data_date
                    FROM training_labels tl
                    JOIN watches w ON w.id = tl.watch_id
                    WHERE w.hardware_id = %s
                """
                params = [device_id]

                # Filter by user if provided
                if user_id:
                    query += " AND tl.participant_id = %s"
                    params.append(int(user_id))

                if start_date:
                    query += " AND DATE(tl.created_at) >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND DATE(tl.created_at) <= %s"
                    params.append(end_date)

                query += " ORDER BY data_date ASC"

                cursor.execute(query, params)
                results = cursor.fetchall()

                return [row[0].strftime('%Y-%m-%d') for row in results if row[0]]
    except Exception as e:
        print(f"Error querying PostgreSQL for daily availability: {e}")
        return []

@app.route('/api/custom-enrollment', methods=['GET'])
@requires_auth
def get_all_custom_enrollments():
    """Get all custom enrollment periods"""
    try:
        conn = get_settings_db()
        rows = conn.execute("SELECT user_id, device_id, start_date, end_date FROM custom_enrollment_periods").fetchall()
        conn.close()

        result = {}
        for row in rows:
            key = f"{row['user_id']}-{row['device_id']}"
            result[key] = {
                'user_id': row['user_id'],
                'device_id': row['device_id'],
                'start_date': row['start_date'],
                'end_date': row['end_date']
            }
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-enrollment/<int:user_id>/<device_id>', methods=['PUT'])
@requires_auth
def save_custom_enrollment(user_id, device_id):
    """Save or update a custom enrollment period"""
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400

        conn = get_settings_db()
        conn.execute("""
            INSERT INTO custom_enrollment_periods (user_id, device_id, start_date, end_date, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user_id, device_id) DO UPDATE SET
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                updated_at = datetime('now')
        """, (user_id, device_id, start_date, end_date))
        conn.commit()
        conn.close()

        return jsonify({'message': 'Custom enrollment period saved', 'user_id': user_id, 'device_id': device_id}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-enrollment/<int:user_id>/<device_id>', methods=['DELETE'])
@requires_auth
def delete_custom_enrollment(user_id, device_id):
    """Delete a custom enrollment period (reset to original)"""
    try:
        conn = get_settings_db()
        conn.execute("DELETE FROM custom_enrollment_periods WHERE user_id = ? AND device_id = ?", (user_id, device_id))
        conn.commit()
        conn.close()

        return jsonify({'message': 'Custom enrollment period deleted'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/validation/trigger', methods=['POST'])
@requires_auth
def trigger_validation():
    """Trigger validation run"""
    global _validation_running

    try:
        with _validation_lock:
            if _validation_running:
                return jsonify({'error': 'Validation is already running'}), 400

            _validation_running = True

        # Run validation in background thread
        def run_validation_background():
            global _validation_running
            try:
                run_validation(days_back=7, max_workers=4)
            except Exception as e:
                print(f"Validation error: {e}")
            finally:
                with _validation_lock:
                    _validation_running = False

        thread = threading.Thread(target=run_validation_background, daemon=True)
        thread.start()

        return jsonify({'message': 'Validation started'}), 200
    except Exception as e:
        with _validation_lock:
            _validation_running = False
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    """API documentation endpoint"""
    return jsonify({
        'message': 'LemurDX Data Export API',
        'endpoints': {
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
            '/health': {
                'methods': ['GET'],
                'description': 'Health check endpoint'
            }
        }
    }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5012)

