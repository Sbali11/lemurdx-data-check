# Participant Data Download Feature

## Overview
A new page has been added to the LemurDX admin dashboard that allows downloading data for multiple participants at once, organized by sensor type.

## Location
- **URL**: `/admin/participant-download`
- **Template**: `templates/participant_download.html`
- **Route**: Added to `app.py` as `admin_participant_download()`

## Features

### 1. Participant Selection
- View all participants in a table with their devices and date ranges
- Select multiple participants using checkboxes
- "Select All" checkbox for bulk selection
- Shows which participants have custom enrollment dates (yellow badge)

### 2. Filtering Options
- **Filter by Custom Dates**: Toggle to show only participants who have custom enrollment periods set
- Helps identify participants with specific date configurations

### 3. Sensor-Based Download
Four buttons for downloading data by sensor type:
- **Motion Data** - acceleration, gravity, rotation, magnetic field, orientation
- **Heart Rate Data** - heart rate measurements
- **Location Data** - GPS coordinates and altitude
- **Label Data** - training labels and activities

### 4. Date Handling
- **Custom Dates**: If a participant has custom enrollment dates set, those dates are automatically used for the download
- **Default Dates**: If no custom dates are set, all available data for that participant is downloaded
- Each CSV file includes timestamps in every row

### 5. Batch Processing
- Downloads are processed sequentially to avoid overwhelming the server
- Progress is displayed in real-time showing:
  - Which file is being downloaded
  - Success/failure status for each download
  - Final summary of successful and failed downloads

### 6. File Naming Convention
CSV files are named with the following format:
```
{user_email}_{device_id}_{sensor_type}_{date}.csv
```

Example: `john_9001_motion_data_2026-03-04.csv`

## How It Works

1. **Load Participants**: 
   - Fetches all users from `/users` endpoint
   - Loads custom enrollment dates from `/api/custom-enrollment`
   - Fetches devices for each user from `/api/user/{id}/devices`

2. **Apply Filters**:
   - Optional filter shows only participants with custom dates

3. **Select Participants**:
   - User selects one or more participants from the table

4. **Download by Sensor**:
   - User clicks one of the sensor buttons
   - For each selected participant and each of their devices:
     - Calls `/export` endpoint with appropriate parameters
     - If custom dates exist, includes them in the request
     - Downloads the CSV file with a unique filename

5. **Progress Tracking**:
   - Shows real-time progress for each file
   - Displays success (green) or failure (red) status
   - Provides final summary

## API Integration

The feature uses existing API endpoints:
- `GET /users` - Get all users
- `GET /api/user/{id}/devices` - Get devices for a user
- `GET /api/custom-enrollment` - Get all custom enrollment periods
- `GET /export` - Export sensor data (with parameters: device_id, measure_name, start_time, end_time)

## Benefits

1. **Bulk Operations**: Download data for multiple participants at once
2. **Organized Output**: Separate CSV files per participant and device
3. **Date-Aware**: Automatically respects custom enrollment dates
4. **Flexible**: Can filter participants and choose specific sensors
5. **Transparent**: Clear progress tracking and error reporting
6. **Date Inclusion**: Each CSV row contains timestamp data for analysis

## Access

The page is accessible from the main admin dashboard at `/admin` under the "Participant Data Download" card.

## Security

- Requires authentication (HTTP Basic Auth)
- Uses same authentication as other admin pages
- All API calls include credentials
