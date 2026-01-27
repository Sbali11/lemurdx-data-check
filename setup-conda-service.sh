#!/bin/bash

# Setup script for LemurDX Data Check with Conda
set -e

echo "=== LemurDX Data Check - Conda Setup ==="
echo ""

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "ERROR: conda is not installed or not in PATH"
    echo "Please install Miniconda or Anaconda first"
    exit 1
fi

# Environment name
ENV_NAME="lemurdx-data"

# Check if environment already exists
if conda env list | grep -q "^${ENV_NAME} "; then
    echo "✓ Conda environment '${ENV_NAME}' already exists"
    read -p "Do you want to recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing environment..."
        conda env remove -n ${ENV_NAME} -y
    else
        echo "Using existing environment"
    fi
fi

# Create conda environment if it doesn't exist
if ! conda env list | grep -q "^${ENV_NAME} "; then
    echo "Creating conda environment '${ENV_NAME}' with Python 3.10..."
    conda create -n ${ENV_NAME} python=3.10 -y
fi

# Get conda base path
CONDA_BASE=$(conda info --base)
CONDA_ENV_PATH="${CONDA_BASE}/envs/${ENV_NAME}"

echo ""
echo "✓ Conda environment path: ${CONDA_ENV_PATH}"
echo ""

# Activate environment and install requirements
echo "Installing requirements..."
source "${CONDA_BASE}/etc/profile.d/conda.sh"
conda activate ${ENV_NAME}

pip install -r requirements.txt

echo ""
echo "✓ Dependencies installed"
echo ""

# Update service file with correct conda path
SERVICE_FILE="lemurdx-data-check.service"
SERVICE_FILE_UPDATED="${SERVICE_FILE}.updated"

echo "Updating systemd service file..."

# Replace placeholder paths with actual conda environment path
sed "s|/home/shreya/miniconda3/envs/lemurdx-data|${CONDA_ENV_PATH}|g" ${SERVICE_FILE} > ${SERVICE_FILE_UPDATED}

echo "✓ Service file updated: ${SERVICE_FILE_UPDATED}"
echo ""

# Show the service file
echo "=== Updated Service File ==="
cat ${SERVICE_FILE_UPDATED}
echo ""
echo "==========================="
echo ""

# Ask if user wants to install the service
read -p "Install and enable the systemd service? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Installing service..."
    sudo cp ${SERVICE_FILE_UPDATED} /etc/systemd/system/${SERVICE_FILE}
    sudo systemctl daemon-reload
    sudo systemctl enable ${SERVICE_FILE}
    sudo systemctl restart ${SERVICE_FILE}

    echo ""
    echo "✓ Service installed and started"
    echo ""

    # Show status
    sudo systemctl status ${SERVICE_FILE} --no-pager

    echo ""
    echo "=== Service Commands ==="
    echo "Start:   sudo systemctl start lemurdx-data-check"
    echo "Stop:    sudo systemctl stop lemurdx-data-check"
    echo "Restart: sudo systemctl restart lemurdx-data-check"
    echo "Status:  sudo systemctl status lemurdx-data-check"
    echo "Logs:    sudo journalctl -u lemurdx-data-check -f"
    echo ""
else
    echo ""
    echo "Service not installed. To install manually:"
    echo "  sudo cp ${SERVICE_FILE_UPDATED} /etc/systemd/system/${SERVICE_FILE}"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable ${SERVICE_FILE}"
    echo "  sudo systemctl start ${SERVICE_FILE}"
    echo ""
fi

# Test the application
echo "=== Testing Application ==="
echo "To test manually, run:"
echo "  conda activate ${ENV_NAME}"
echo "  cd $(pwd)"
echo "  gunicorn --workers 3 --bind 127.0.0.1:5008 wsgi:app"
echo ""
echo "To test the health endpoint:"
echo "  curl http://127.0.0.1:5008/lemurdx-dashboard-data/health"
echo ""

echo "✓ Setup complete!"
