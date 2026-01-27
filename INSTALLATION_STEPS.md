# LemurDX Data Check - Installation Guide

## Prerequisites
- Conda environment `lemurdx-data` already exists with all packages installed ✓
- Service file [lemurdx-dashboard-data.service](lemurdx-dashboard-data.service) updated with security fixes ✓
- Environment variables configured in [.env](.env) ✓

## Step 1: Configure Nginx

You need to add the `/lemurdx-dashboard-data` location block to your existing nginx configuration.

### 1.1 Edit the existing nginx config

```bash
sudo nano /etc/nginx/sites-available/default
```

### 1.2 Add this location block inside the existing `server { }` block

Find the server block that has `listen 443 ssl` and `server_name big1.lan.cmu.edu` (around line 75). You'll see existing location blocks:

```nginx
server {
    listen 443 ssl;
    server_name big1.lan.cmu.edu;

    # ... SSL config ...

    location /lemurdx { ... }
    location /lemurdx/watch_upload { ... }
    location /prism { ... }
    location /cleveland { ... }
    location /cleveland_dashboard { ... }
    location /prism_chat { ... }

    # ADD THE NEW LOCATION BLOCK HERE (before the closing })
}
```

Add this new location block AFTER `/prism_chat` and BEFORE the closing `}`:

```nginx
    location /lemurdx-dashboard-data {
        proxy_pass http://127.0.0.1:5008;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Prefix /lemurdx-dashboard-data;

        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
```

**Important**: Add this INSIDE the same server block (proper indentation matters!).

### 1.3 Remove the conflicting nginx config

```bash
sudo rm /etc/nginx/sites-enabled/lemurdx-dashboard-data_nginx.conf
sudo rm /etc/nginx/sites-available/lemurdx-dashboard-data_nginx.conf
```

### 1.4 Test and reload nginx

```bash
# Test configuration for errors
sudo nginx -t

# If test passes, reload nginx
sudo systemctl reload nginx
```

## Step 2: Install the Systemd Service

### 2.1 Copy service file to systemd directory

```bash
sudo cp /home/shreya/lemurdx-data-check/lemurdx-dashboard-data.service /etc/systemd/system/
```

### 2.2 Reload systemd daemon

```bash
sudo systemctl daemon-reload
```

### 2.3 Enable service to start on boot

```bash
sudo systemctl enable lemurdx-dashboard-data
```

### 2.4 Start the service

```bash
sudo systemctl start lemurdx-dashboard-data
```

### 2.5 Check service status

```bash
sudo systemctl status lemurdx-dashboard-data
```

You should see `active (running)` in green.

## Step 3: Verification

### 3.1 Check service logs

```bash
sudo journalctl -u lemurdx-dashboard-data -n 50
```

You should see output like:
```
[INFO] Starting gunicorn 23.0.0
[INFO] Listening at: http://127.0.0.1:5008
[INFO] Using worker: sync
[INFO] Booting worker with pid: ...
```

### 3.2 Test local endpoint

```bash
curl http://127.0.0.1:5008/lemurdx-dashboard-data/health
```

Expected output: `{"status":"healthy"}`

### 3.3 Test public URL (without auth)

```bash
curl https://big1.lan.cmu.edu/lemurdx-dashboard-data/health
```

Expected output: `{"status":"healthy"}`

### 3.4 Test admin interface (with auth)

Open in browser: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin`

You should see a login prompt. Use credentials from [.env](.env):
- Username: `sbali`
- Password: `lemurdx8743`

After login, you should see the admin dashboard.

## Common Commands

### View service status
```bash
sudo systemctl status lemurdx-dashboard-data
```

### View real-time logs
```bash
sudo journalctl -u lemurdx-dashboard-data -f
```

### Restart service (after code changes)
```bash
sudo systemctl restart lemurdx-dashboard-data
```

### Stop service
```bash
sudo systemctl stop lemurdx-dashboard-data
```

### Disable service from auto-start
```bash
sudo systemctl disable lemurdx-dashboard-data
```

### Check if service is listening on port
```bash
ss -tlnp | grep 5008
```

## Troubleshooting

### Service fails to start

**Check logs:**
```bash
sudo journalctl -u lemurdx-dashboard-data -n 100 --no-pager
```

**Common issues:**
- Conda environment path incorrect: Verify with `ls /home/shreya/miniconda3/envs/lemurdx-data/bin/gunicorn`
- .env file permissions: `ls -la /home/shreya/lemurdx-data-check/.env`
- Port already in use: `ss -tlnp | grep 5008`

### Nginx shows 502 Bad Gateway

**Check if service is running:**
```bash
systemctl status lemurdx-dashboard-data
```

**Check nginx error logs:**
```bash
sudo tail -f /var/log/nginx/error.log
```

**Verify port is listening:**
```bash
ss -tlnp | grep 5008
```

### Authentication not working

**Verify credentials in .env:**
```bash
cat /home/shreya/lemurdx-data-check/.env | grep ADMIN
```

**Restart service after changing .env:**
```bash
sudo systemctl restart lemurdx-dashboard-data
```

### Database connection errors

**Verify database variables in .env:**
```bash
cat /home/shreya/lemurdx-data-check/.env | grep DB_
```

**Test database connection manually:**
```bash
# Activate conda environment
source /home/shreya/miniconda3/bin/activate lemurdx-data

# Try connecting with psql
psql -h terraform-20241216062211624600000001.c3gooskii1m3.us-east-1.rds.amazonaws.com \
     -U pipeline_user \
     -d pipeline_database \
     -p 5432
```

## Security Notes

- ✓ Admin credentials are loaded from `.env` file (not hardcoded in service file)
- ✓ Database credentials are loaded from `.env` file
- ✓ AWS credentials can be added to `.env` file if needed
- ✓ Service runs as user `shreya` with group `www-data` (limited permissions)
- ⚠️ Ensure `.env` file has proper permissions: `chmod 640 /home/shreya/lemurdx-data-check/.env`
- ⚠️ Never commit `.env` file to git (already in .gitignore)

## URL Structure

Once configured, your application will be accessible at:

- Health check: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/health`
- Admin dashboard: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin`
- Export interface: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin/export`
- Validation interface: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/admin/validation`
- API docs: `https://big1.lan.cmu.edu/lemurdx-dashboard-data/`

The URL prefix `/lemurdx-dashboard-data` is handled by:
1. Nginx receives request at `/lemurdx-dashboard-data/*`
2. Nginx proxies to `http://127.0.0.1:5008`
3. Gunicorn receives request with prefix intact
4. Flask app handles the request through [wsgi.py](wsgi.py) DispatcherMiddleware
