import argparse
import time
from datetime import datetime, timedelta
from mpi4py import MPI
import psutil
from utils import fetch_and_save, parse_html_string, update_csv, BODY_TEMPLATE
import requests
import glob
import os


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield (start_date + timedelta(n)).strftime('%Y%m%d')

def fetch_html(date_str):
    url = "https://www.moex.com/ru/derivatives/open-positions-new.aspx/open-positions-csv.aspx"
    params = {"d": date_str, "t": "1"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    target_dt = datetime.strptime(date_str, "%Y%m%d")
    adjusted_dt = target_dt - timedelta(days=1)
    day = adjusted_dt.day
    month = adjusted_dt.month
    year = adjusted_dt.year
    data = BODY_TEMPLATE.format(date=date_str, day=day, month=month, year=year)
    response = requests.post(url, params=params, headers=headers, data=data)
    if response.status_code == 200:
        return response.content.decode("utf-8")
    else:
        return None

def main():
    parser = argparse.ArgumentParser(description='Parallel MOEX data loading (MPI)',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--start', required=True, help='Start date (YYYYMMDD)')
    parser.add_argument('--end', required=True, help='End date (YYYYMMDD)')
    parser.add_argument('--csv', default='output_MIX.csv', help='Output CSV file name')
    parser.add_argument('--clean', action='store_true', help='Delete all CSV files in the directory before run')
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if args.clean and rank == 0:
        for f in glob.glob('*.csv'):
            try:
                os.remove(f)
                print(f'Removed file: {f}')
            except Exception as e:
                print(f'Error removing {f}: {e}')
        print('All CSV files deleted.\n')
    if args.clean:
        comm.Barrier()

    start_dt = datetime.strptime(args.start, '%Y%m%d')
    end_dt = datetime.strptime(args.end, '%Y%m%d')
    all_dates = list(daterange(start_dt, end_dt))
    num_dates = len(all_dates)

    my_dates = all_dates[rank::size]

    if rank == 0:
        print(f'Processing dates from {args.start} to {args.end} ({num_dates} days) with {size} MPI processes...')
        cpu_percent_before = psutil.cpu_percent(interval=None)
        t0 = time.time()

    my_results = []
    for date_str in my_dates:
        html = fetch_html(date_str)
        if html:
            trade_date, data_rows = parse_html_string(html)
            if trade_date and data_rows:
                my_results.append((trade_date, data_rows))

    gathered = comm.gather(my_results, root=0)

    if rank == 0:
        written_dates = set()
        for proc_results in gathered:
            for trade_date, data_rows in proc_results:
                if trade_date not in written_dates:
                    update_csv(args.csv, trade_date, data_rows)
                    written_dates.add(trade_date)
        t1 = time.time()
        cpu_percent_after = psutil.cpu_percent(interval=None)
        elapsed = t1 - t0
        throughput = num_dates / elapsed if elapsed > 0 else 0
        print('\n==== Results ====' )
        print(f'Time elapsed: {elapsed:.2f} sec')
        print(f'Average throughput: {throughput:.2f} days/sec')
        print(f'CPU usage (before): {cpu_percent_before}%, CPU usage (after): {cpu_percent_after}%')
        print(f'Result file: {args.csv}')

if __name__ == '__main__':
    main()