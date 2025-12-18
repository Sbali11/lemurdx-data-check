from flask import Flask, request, send_file, jsonify, render_template, Response
from functools import wraps
import os
import tempfile
import threading
import csv
from io import StringIO
from export import export_sensor_data_to_csv, load_all_config, get_db_connection, get_timestream_date_range, get_postgres_label_date_range
from validate_data import get_all_users_with_devices, load_validation_results, run_validation
from datetime import datetime

app = Flask(__name__)

# Admin credentials (can be overridden via environment variables)
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD')  # Change this in production!

# Track if validation is running
_validation_lock = threading.Lock()
_validation_running = False

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
        
        if results is None:
            # Check if validation is running
            with _validation_lock:
                is_running = _validation_running
            
            if is_running:
                return jsonify({
                    'is_running': True,
                    'status': 'running',
                    'users': [],
                    'message': 'Validation is running...'
                }), 202
            
            return jsonify({
                'message': 'No validation results found. Click "Run Now" to start validation.',
                'last_run': None
            }), 404
        
        # Check if validation is still running
        with _validation_lock:
            is_running = _validation_running
        
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

@app.route('/api/validation/trigger', methods=['POST'])
@requires_auth
def trigger_validation():
    """Trigger validation run"""
    try:
        with _validation_lock:
            if _validation_running:
                return jsonify({'error': 'Validation is already running'}), 400
            
            _validation_running = True
        
        # Run validation in background thread
        def run_validation_background():
            try:
                run_validation(days_back=7, max_workers=4)
            except Exception as e:
                print(f"Validation error: {e}")
            finally:
                with _validation_lock:
                    global _validation_running
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
    app.run(debug=True, host='0.0.0.0', port=5000)

