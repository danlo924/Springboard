import glob
import os.path as p
from datetime import datetime, timedelta
from secrets import secrets

airflow_logs = secrets.get("root_dir")
lookback_hours = 120                    # 5 day lookback window = 120 hours

def get_error_logs(file_dir, lookback_hours):
    """recursively find all files within a given file directory that have been modified within lookback_hours
    and pass them to the analyze_file function; print final error counts and error lines retrieved from all flies"""
    tot_count = 0
    all_errors = []
    files = glob.glob(file_dir + "/**/*.log", recursive=True)
    for f in files:
        f_mod_date = datetime.fromtimestamp(p.getmtime(f))
        date_thresh = datetime.now() - timedelta(hours=lookback_hours)
        if f_mod_date >= date_thresh:
            count, curr_list = analyze_file(f)
            if count > 0:
                tot_count += count
                all_errors.append(["File: " + f + ", Modified Date: " + f_mod_date.strftime('%Y-%m-%d %H:%M:%S') + ", Error Count: " + str(count)])
                all_errors.append(curr_list)
    print("Total number of errors: " + str(tot_count))
    print("Here are all the errors:\n")
    for errors in all_errors:
        print(*errors)

def analyze_file(file):
    """open file and reach each line to check for errors; count number of lines with errors, and return lines with errors as list"""
    err_count = 0
    err_list = []
    with open(file) as f:
        lines = f.readlines()
        for l in lines:
            if "ERROR" in l:
                err_count += 1
                err_list.append(l)
    return err_count, err_list

if __name__ == "__main__":
    get_error_logs(airflow_logs, lookback_hours)