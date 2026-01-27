# Quick Start Guide - LemurDX Data Check

## TL;DR - Run These Commands

### 1. Add nginx configuration
```bash
# Edit the default nginx config
sudo nano /etc/nginx/sites-available/default

# Find the server block with "listen 443 ssl" and "server_name big1.lan.cmu.edu"
# Scroll to the bottom where you see "location /prism_chat { ... }"
# Add the location block from NGINX_CONFIG_TO_ADD.txt right after it

# Save and exit (Ctrl+O, Enter, Ctrl+X)

# Remove conflicting config
sudo rm /etc/nginx/sites-enabled/lemurdx-dashboard-data_nginx.conf
sudo rm /etc/nginx/sites-available/lemurdx-dashboard-data_nginx.conf

# Test and reload
sudo nginx -t
sudo systemctl reload nginx
```

### 2. Install and start the service
```bash
# Copy service file
sudo cp /home/shreya/lemurdx-data-check/lemurdx-dashboard-data.service /etc/systemd/system/

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable lemurdx-dashboard-data
sudo systemctl start lemurdx-dashboard-data

# Check status
sudo systemctl status lemurdx-dashboard-data
```

### 3. Verify it works
```bash
# Test health endpoint
curl https://big1.lan.cmu.edu/lemurdx-dashboard-data/health

# Should return: {"status":"healthy"}
```

### 4. Access the admin interface
Open in browser: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin`

Login credentials (from [.env](.env)):
- Username: `sbali`
- Password: `lemurdx8743`

---

## Files Reference

- **[INSTALLATION_STEPS.md](INSTALLATION_STEPS.md)** - Complete installation guide with troubleshooting
- **[NGINX_CONFIG_TO_ADD.txt](NGINX_CONFIG_TO_ADD.txt)** - Exact nginx configuration block to copy/paste
- **[lemurdx-dashboard-data.service](lemurdx-dashboard-data.service)** - Systemd service file (already configured)
- **[CONDA_SETUP.md](CONDA_SETUP.md)** - Conda environment setup (already done ✓)

---

## Useful Commands

```bash
# View service status
sudo systemctl status lemurdx-dashboard-data

# View real-time logs
sudo journalctl -u lemurdx-dashboard-data -f

# Restart service (after code changes)
sudo systemctl restart lemurdx-dashboard-data

# Stop service
sudo systemctl stop lemurdx-dashboard-data
```

---

## URLs

Once setup is complete, your application will be available at:

- **Health check**: https://big1.lan.cmu.edu/lemurdx-dashboard-data/health
- **Admin dashboard**: https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin
- **Export interface**: https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin/export
- **Validation interface**: https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin/validation

---

## Architecture

```
User Request → Nginx (port 443) → Gunicorn (port 5008) → Flask App
                 ↓
      /lemurdx-dashboard-data/*
                 ↓
       proxies to 127.0.0.1:5008
```

---

## If Something Goes Wrong

See [INSTALLATION_STEPS.md](INSTALLATION_STEPS.md) for detailed troubleshooting.

Quick checks:
```bash
# Is service running?
systemctl status lemurdx-dashboard-data

# Is it listening on port 5008?
ss -tlnp | grep 5008

# Check service logs
sudo journalctl -u lemurdx-dashboard-data -n 50

# Check nginx logs
sudo tail -f /var/log/nginx/error.log
```
