# Data Validation Table

## Expected Data Formats

| Data Type | Expected Fields | Field Types | Data Structure |
|-----------|----------------|-------------|----------------|
| **motion_data** | `time`, `acceleration_x`, `acceleration_y`, `acceleration_z`, `gravity_x`, `gravity_y`, `gravity_z`, `rotation_x`, `rotation_y`, `rotation_z`, `mag_field_x`, `mag_field_y`, `mag_field_z`, `roll`, `pitch`, `yaw`, `quaternion_x`, `quaternion_y`, `quaternion_z`, `quaternion_w` | All numeric (float), `time` (timestamp) | Time-series sensor data |
| **heart_rate_data** | `time`, `heart_rate` | `heart_rate` numeric (float/int), `time` (timestamp) | Time-series sensor data |
| **location_data** | `time`, `latitude`, `longitude`, `altitude` | All numeric (float), `time` (timestamp) | Time-series GPS data |
| **label_data** | `device_id`, `uid`, `created_at`, `completed_at`, `enjoyment`, `note`, `training_activity_name` | `device_id`, `uid` (string), `created_at`, `completed_at` (datetime), `enjoyment` (numeric), `note`, `training_activity_name` (string) | Event-based label data |

## Validation Checks by Data Type

| Data Type | Data Source | Required Fields | Missing Values Check | Format Validation | Value Range Checks | Data Sampling | Sampling Statistics | Gap Detection |
|-----------|-------------|-----------------|---------------------|-------------------|-------------------|---------------|-------------------|--------------|
| **motion_data** | AWS Timestream | `acceleration_x`, `acceleration_y`, `acceleration_z` | All fields checked for null/empty | All numeric fields validated | `acceleration_x/y/z`: -20 to 20 m/s²<br>`quaternion_x/y/z/w`: -1 to 1 | ⚠ **Sampled** if >30 days:<br>• Uses COUNT(*) for total<br>• Samples up to 10K rows<br>• Falls back to 30-day chunks if needed | ✓ Calculated<br>• Sampling frequency (Hz)<br>• Sampling period (seconds)<br>• Median period | ✓ Detected<br>• Gaps >3x median period<br>• Gap count & total duration |
| **heart_rate_data** | AWS Timestream | `heart_rate` | `heart_rate` checked for null/empty | `heart_rate` validated as numeric | `heart_rate`: 30 to 220 bpm | ⚠ **Sampled** if >30 days:<br>• Uses COUNT(*) for total<br>• Samples up to 10K rows<br>• Falls back to 30-day chunks if needed | ✓ Calculated<br>• Sampling frequency (Hz)<br>• Sampling period (seconds)<br>• Median period | ✓ Detected<br>• Gaps >3x median period<br>• Gap count & total duration |
| **location_data** | AWS Timestream | `latitude`, `longitude` | `latitude`, `longitude` checked for null/empty | `latitude`, `longitude`, `altitude` validated as numeric | `latitude`: -90 to 90<br>`longitude`: -180 to 180<br>`altitude`: -500 to 10,000 meters | ⚠ **Sampled** if >30 days:<br>• Uses COUNT(*) for total<br>• Samples up to 10K rows<br>• Falls back to 30-day chunks if needed | ✓ Calculated<br>• Sampling frequency (Hz)<br>• Sampling period (seconds)<br>• Median period | ✓ Detected<br>• Gaps >3x median period<br>• Gap count & total duration |
| **label_data** | PostgreSQL | `device_id`, `uid`, `created_at` | All required fields checked for null/empty | `enjoyment` validated as numeric<br>`created_at`, `completed_at` validated as dates | `enjoyment`: 0 to 10 | ✓ Full dataset queried<br>(typically small volume) | ✓ Calculated<br>• Event frequency<br>• Time between events<br>• Median period | ✓ Detected<br>• Gaps >3x median period<br>• Gap count & total duration |

## Validation Details

### Common Checks (All Data Types)
- **Total Row Count**: Number of data points available
- **Date Range**: Min/max timestamps of available data
- **Format Errors**: Up to 1,000 rows sampled for format validation
- **Error Handling**: Full error messages captured if queries fail

### Format Validation Rules
- **Numeric Fields**: Must be valid numbers (float conversion)
- **Range Validation**: Values checked against expected min/max ranges
- **Required Fields**: Must be present and non-empty
- **Date Fields**: Validated for proper date format (label_data only)
- **Format Check Sampling**: Up to 1,000 rows sampled for format validation (to limit processing time)

### Data Sampling Strategy
- **Large Datasets (>30 days)**: 
  - Uses `COUNT(*)` aggregation query to get accurate total row count
  - Samples up to 10,000 rows for validation (format checks, gap detection)
  - Falls back to 30-day chunked approach if query size limits exceeded
  - Results include both `total_rows` (actual count) and `sample_size` (rows used for validation)
- **Medium Datasets (≤30 days)**: 
  - Uses `LIMIT 50,000` to cap query results
  - Full dataset used for validation
- **Small Datasets**: 
  - Full dataset queried and validated
- **Note**: Sampling is transparent - total counts are accurate, validation uses representative sample

### Sampling Statistics
- **Sampling Frequency**: Calculated as 1 / average_period (Hz)
- **Sampling Period**: Average time between consecutive data points
- **Median Period**: More robust than mean, used for gap detection threshold

### Gap Detection
- **Threshold**: Gaps detected when time between samples > 3x median period
- **Gap Info**: Start time, end time, duration for each gap
- **Total Gap Time**: Sum of all gap durations
- **Gap Count**: Number of interruptions detected

### Large Dataset Handling
- **>30 days**: Uses COUNT(*) aggregation + samples up to 10K rows
- **≤30 days**: Uses LIMIT 50K
- **Fallback**: Chunks into 30-day periods if query size limits exceeded

### Incremental Validation
- **First Run**: Validates all available data from beginning
- **Subsequent Runs**: Only validates new data since last check
- **History Tracking**: Maintains date ranges checked per user/device/modality
