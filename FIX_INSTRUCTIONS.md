# Fix Instructions for LemurDX Dashboard Data Check

## Issues Fixed:
1. Changed port from 5006 to 5008 (5006 was already in use)
2. Fixed nginx proxy_pass configuration

## Steps to Fix:

### 1. Update the systemd service file:
```bash
sudo cp /home/shreya/lemurdx-data-check/lemurdx-data-check.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 2. Update credentials in service file:
```bash
sudo nano /etc/systemd/system/lemurdx-data-check.service
# Update ADMIN_USERNAME and ADMIN_PASSWORD (lines 10-11)
```

### 3. Stop any old service and start the new one:
```bash
sudo systemctl stop lemurdx-data-check.service 2>/dev/null || true
sudo systemctl start lemurdx-data-check.service
sudo systemctl status lemurdx-data-check.service
```

### 4. Update nginx configuration:
```bash
sudo cp /home/shreya/lemurdx-data-check/lemurdx-dashboard-data_nginx.conf /etc/nginx/sites-available/
sudo nginx -t  # Test configuration
sudo systemctl reload nginx
```

### 5. Test the endpoint:
```bash
curl -I http://127.0.0.1:5008/lemurdx-dashboard-data/health
```

### 6. Check logs if issues persist:
```bash
# Service logs
sudo journalctl -u lemurdx-data-check.service -f

# Nginx logs
sudo tail -f /var/log/nginx/lemurdx-dashboard-data_error.log
sudo tail -f /var/log/nginx/lemurdx-dashboard-data_access.log
```

## Expected Result:
After setup, the app should be accessible at:
- https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin
- https://big1.lan.cmu.edu/lemurdx-dashboard-data/health

