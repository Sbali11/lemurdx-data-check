#!/usr/bin/env python3
"""
Export sensor data for all participants with custom enrollment periods.
Exports day-by-day to avoid Timestream timeouts.

Usage:
    python export_all_custom.py [--sensors motion_data,heart_rate_data] [--output-dir analysis]
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

# Ensure we can import from the project
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from export import export_sensor_data_to_csv, load_all_config, get_db_connection

SETTINGS_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.db')

ALL_SENSORS = ['motion_data', 'heart_rate_data', 'location_data']


def get_custom_enrollments():
    """Read all custom enrollment periods from settings.db"""
    conn = sqlite3.connect(SETTINGS_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT user_id, device_id, start_date, end_date FROM custom_enrollment_periods ORDER BY user_id, device_id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_user_emails(db_config, user_ids):
    """Look up user emails from Postgres"""
    if not user_ids:
        return {}
    with get_db_connection(db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, email FROM users WHERE id IN %s", (tuple(user_ids),))
            return {row[0]: row[1] for row in cursor.fetchall()}


def days_between(start_date, end_date):
    """Yield each date string from start to end inclusive"""
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    while current <= end:
        yield current.strftime('%Y-%m-%d')
        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Export data for all custom enrollment periods")
    parser.add_argument('--sensors', default=','.join(ALL_SENSORS),
                        help=f"Comma-separated sensor types (default: {','.join(ALL_SENSORS)})")
    parser.add_argument('--output-dir', default='analysis',
                        help="Output directory (default: analysis)")
    args = parser.parse_args()

    sensors = [s.strip() for s in args.sensors.split(',')]
    output_base = args.output_dir

    # Load custom enrollments
    enrollments = get_custom_enrollments()
    if not enrollments:
        print("No custom enrollment periods found.")
        return

    print(f"Found {len(enrollments)} custom enrollment(s)")
    print(f"Sensors: {', '.join(sensors)}")
    print(f"Output: {output_base}/")
    print()

    # Get user emails
    config = load_all_config()
    user_ids = list(set(e['user_id'] for e in enrollments))
    emails = get_user_emails(config['db'], user_ids)

    total_success = 0
    total_nodata = 0
    total_fail = 0

    for i, enrollment in enumerate(enrollments, 1):
        uid = enrollment['user_id']
        device = enrollment['device_id']
        start = enrollment['start_date']
        end = enrollment['end_date']
        email = emails.get(uid, f'user_{uid}')
        username = email.split('@')[0] if '@' in email else email

        days = list(days_between(start, end))

        print(f"[{i}/{len(enrollments)}] {email} | device {device} | {start} to {end} ({len(days)} days)")

        for sensor in sensors:
            # Create output dir: analysis/username_device/
            user_dir = os.path.join(output_base, f"{username}_{device}")
            os.makedirs(user_dir, exist_ok=True)

            success = 0
            nodata = 0
            fail = 0

            for day_num, day in enumerate(days, 1):
                outfile = os.path.join(user_dir, f"{sensor}_{day}.csv")

                # Skip if already exported
                if os.path.exists(outfile) and os.path.getsize(outfile) > 0:
                    success += 1
                    continue

                try:
                    export_sensor_data_to_csv(
                        device_id=device,
                        output_file=outfile,
                        measure_name=sensor,
                        start_time=f"{day} 00:00:00",
                        end_time=f"{day} 23:59:59"
                    )
                    if os.path.exists(outfile):
                        success += 1
                    else:
                        nodata += 1
                except Exception as e:
                    fail += 1
                    # Remove partial files
                    if os.path.exists(outfile):
                        os.remove(outfile)

            status = f"  {sensor}: {success} ok, {nodata} empty, {fail} failed"
            print(status)

            total_success += success
            total_nodata += nodata
            total_fail += fail

    print()
    print(f"Total: {total_success} exported, {total_nodata} empty, {total_fail} failed")


if __name__ == '__main__':
    main()
