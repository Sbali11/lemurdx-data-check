# Conda Environment Setup for LemurDX Data Check

## Step 1: Create Conda Environment

```bash
# Create a new conda environment named 'lemurdx-data'
conda create -n lemurdx-data python=3.10 -y

# Activate the environment
conda activate lemurdx-data
```

## Step 2: Install Dependencies

```bash
# Navigate to project directory
cd /home/shreya/lemurdx-data-check

# Install all required packages
pip install -r requirements.txt
```

## Step 3: Verify Installation

```bash
# Check that all packages are installed
pip list

# Test the application locally
python app.py
```

Press Ctrl+C to stop the test server once verified.

## Step 4: Find Conda Environment Path

```bash
# Find the path to your conda environment
conda env list

# Or get the specific path
conda info --envs | grep lemurdx-data
```

The path should look like: `/home/shreya/miniconda3/envs/lemurdx-data` or `/home/shreya/anaconda3/envs/lemurdx-data`

## Step 5: Update Systemd Service File

The systemd service file needs to use the conda environment's Python and gunicorn.

Edit `lemurdx-data-check.service` to use conda paths:

```ini
[Unit]
Description=Gunicorn instance to serve LemurDX Data Check dashboard
After=network.target

[Service]
User=shreya
Group=www-data
WorkingDirectory=/home/shreya/lemurdx-data-check
EnvironmentFile=/home/shreya/lemurdx-data-check/.env
Environment="PATH=/home/shreya/miniconda3/envs/lemurdx-data/bin:/usr/local/bin:/usr/bin:/bin"
Environment="URL_PREFIX=/lemurdx-dashboard-data"
ExecStart=/home/shreya/miniconda3/envs/lemurdx-data/bin/gunicorn --workers 3 --bind 127.0.0.1:5008 wsgi:app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Important:** Replace `/home/shreya/miniconda3/envs/lemurdx-data` with your actual conda environment path from Step 4.

## Step 6: Install and Enable Service

```bash
# Copy service file to systemd directory
sudo cp lemurdx-data-check.service /etc/systemd/system/

# Reload systemd to recognize the new service
sudo systemctl daemon-reload

# Enable the service to start on boot
sudo systemctl enable lemurdx-data-check

# Start the service
sudo systemctl start lemurdx-data-check

# Check service status
sudo systemctl status lemurdx-data-check
```

## Step 7: Verify Service is Running

```bash
# Check if the service is active
sudo systemctl status lemurdx-data-check

# Check logs if there are issues
sudo journalctl -u lemurdx-data-check -n 50 -f

# Test the endpoint
curl http://127.0.0.1:5008/lemurdx-dashboard-data/health
```

## Common Commands

```bash
# Stop the service
sudo systemctl stop lemurdx-data-check

# Restart the service
sudo systemctl restart lemurdx-data-check

# View logs
sudo journalctl -u lemurdx-data-check -f

# Disable service from starting on boot
sudo systemctl disable lemurdx-data-check
```

## Troubleshooting

### Service fails to start

1. Check logs:
   ```bash
   sudo journalctl -u lemurdx-data-check -n 100
   ```

2. Verify conda environment path:
   ```bash
   ls -la /home/shreya/miniconda3/envs/lemurdx-data/bin/gunicorn
   ```

3. Test manually:
   ```bash
   conda activate lemurdx-data
   cd /home/shreya/lemurdx-data-check
   gunicorn --workers 3 --bind 127.0.0.1:5008 wsgi:app
   ```

### Permission issues

```bash
# Ensure proper permissions
sudo chown -R shreya:www-data /home/shreya/lemurdx-data-check
chmod 644 /home/shreya/lemurdx-data-check/.env
```

### AWS credentials

If you're using AWS services (Timestream), ensure AWS credentials are available:

```bash
# Either set in .env file:
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1

# Or ensure ~/.aws/credentials is readable by the service user
```
