#!/bin/bash
# Setup script for LemurDX Data Check systemd service

set -e

SERVICE_NAME="lemurdx-data-check"
SERVICE_FILE="lemurdx-data-check.service"
SYSTEMD_PATH="/etc/systemd/system/${SERVICE_FILE}"

echo "Setting up ${SERVICE_NAME} systemd service..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Install gunicorn if not already installed
echo "Checking for gunicorn..."
if ! command -v gunicorn &> /dev/null; then
    echo "Installing gunicorn..."
    pip3 install gunicorn
else
    echo "Gunicorn already installed at: $(which gunicorn)"
fi

# Find gunicorn path
GUNICORN_PATH=$(which gunicorn)
echo "Using gunicorn at: ${GUNICORN_PATH}"

# Update service file with actual gunicorn path
WORK_DIR="/home/shreya/lemurdx-data-check"
sed -i "s|ExecStart=.*|ExecStart=${GUNICORN_PATH} --workers 3 --bind 127.0.0.1:5006 wsgi:app|" "${WORK_DIR}/${SERVICE_FILE}"

# Copy service file to systemd directory
echo "Copying service file to ${SYSTEMD_PATH}..."
cp "${WORK_DIR}/${SERVICE_FILE}" "${SYSTEMD_PATH}"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

# Enable service
echo "Enabling ${SERVICE_NAME} service..."
systemctl enable "${SERVICE_NAME}.service"

echo ""
echo "Service setup complete!"
echo ""
echo "To start the service:"
echo "  sudo systemctl start ${SERVICE_NAME}.service"
echo ""
echo "To check status:"
echo "  sudo systemctl status ${SERVICE_NAME}.service"
echo ""
echo "To view logs:"
echo "  sudo journalctl -u ${SERVICE_NAME}.service -f"
echo ""
echo "IMPORTANT: Update ADMIN_USERNAME and ADMIN_PASSWORD in ${SYSTEMD_PATH}"
echo "  sudo nano ${SYSTEMD_PATH}"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl restart ${SERVICE_NAME}.service"
echo ""
echo "To set up nginx reverse proxy:"
echo "  1. Copy nginx config:"
echo "     sudo cp ${WORK_DIR}/lemurdx-dashboard-data_nginx.conf /etc/nginx/sites-available/"
echo "  2. Enable it:"
echo "     sudo ln -s /etc/nginx/sites-available/lemurdx-dashboard-data_nginx.conf /etc/nginx/sites-enabled/"
echo "  3. Test and reload nginx:"
echo "     sudo nginx -t"
echo "     sudo systemctl reload nginx"
echo ""
echo "After setup, the app will be accessible at:"
echo "  https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin"

