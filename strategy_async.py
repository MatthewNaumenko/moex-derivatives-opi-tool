import argparse
import asyncio
import aiohttp
import time
import os
import glob
import psutil
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from datetime import datetime, timedelta
from utils import parse_html_string, update_csv, BODY_TEMPLATE

URL = "https://www.moex.com/ru/derivatives/open-positions-new.aspx/open-positions-csv.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
    "Content-Type": "application/x-www-form-urlencoded",
}

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield (start_date + timedelta(n)).strftime('%Y%m%d')

def prepare_post_data(date_str):
    target_dt = datetime.strptime(date_str, "%Y%m%d")
    adjusted_dt = target_dt - timedelta(days=1)
    day = adjusted_dt.day
    month = adjusted_dt.month
    year = adjusted_dt.year
    return BODY_TEMPLATE.format(date=date_str, day=day, month=month, year=year)

async def fetch_html(session, date_str, sem):
    data = prepare_post_data(date_str)
    params = {"d": date_str, "t": "1"}
    async with sem:
        try:
            async with session.post(URL, params=params, headers=HEADERS, data=data, timeout=30) as resp:
                if resp.status == 200:
                    html = await resp.text(encoding="utf-8")
                    return date_str, html
                else:
                    print(f"Error {resp.status} for {date_str}")
                    return date_str, None
        except Exception as e:
            print(f"Exception for {date_str}: {e}")
            return date_str, None

def parse_and_save(args):
    date_str, html, csv_file, lock = args
    if html:
        trade_date, data_rows = parse_html_string(html)
        if trade_date and data_rows:
            update_csv(csv_file, trade_date, data_rows, lock=lock)
            return 1
    return 0

async def main_async(args, all_dates, csv_file, proc_count, concurrent):
    sem = asyncio.Semaphore(concurrent)
    connector = aiohttp.TCPConnector(limit=concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_html(session, date, sem) for date in all_dates]
        results = await asyncio.gather(*tasks)
    return results

def main():
    parser = argparse.ArgumentParser(description='Async+ProcessPool MOEX data loader',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--start', required=True, help='Start date (YYYYMMDD)')
    parser.add_argument('--end', required=True, help='End date (YYYYMMDD)')
    parser.add_argument('--csv', default='output_MIX_async.csv', help='Output CSV file name')
    parser.add_argument('--proc', type=int, default=4, choices=[1,2,4,8,10,12,14], help='Process count (1-14)')
    parser.add_argument('--concurrent', type=int, default=8, help='Max concurrent requests')
    parser.add_argument('--clean', action='store_true', help='Delete all CSV files in the directory before run')
    args = parser.parse_args()

    if args.clean:
        for f in glob.glob('*.csv'):
            try:
                os.remove(f)
                print(f'Removed file: {f}')
            except Exception as e:
                print(f'Error removing {f}: {e}')
        print('All CSV files deleted.\n')

    start_dt = datetime.strptime(args.start, '%Y%m%d')
    end_dt = datetime.strptime(args.end, '%Y%m%d')
    all_dates = list(daterange(start_dt, end_dt))
    num_dates = len(all_dates)

    print(f'Processing dates from {args.start} to {args.end} ({num_dates} days) with {args.proc} processes and {args.concurrent} concurrent requests...')
    cpu_percent_before = psutil.cpu_percent(interval=None)
    t0 = time.time()

    manager = Manager()
    lock = manager.Lock()

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main_async(args, all_dates, args.csv, args.proc, args.concurrent))

    # Парсинг и запись в CSV через процессы
    with ProcessPoolExecutor(max_workers=args.proc) as executor:
        futures = [executor.submit(parse_and_save, (date, html, args.csv, lock)) for date, html in results if html]
        total_written = sum(f.result() for f in futures)

    t1 = time.time()
    cpu_percent_after = psutil.cpu_percent(interval=None)
    elapsed = t1 - t0
    throughput = num_dates / elapsed if elapsed > 0 else 0
    print('\n==== Results ====' )
    print(f'Time elapsed: {elapsed:.2f} sec')
    print(f'Average throughput: {throughput:.2f} days/sec')
    print(f'CPU usage (before): {cpu_percent_before}%, CPU usage (after): {cpu_percent_after}%')
    print(f'Result file: {args.csv}')
    print(f'Total written: {total_written}')

if __name__ == '__main__':
    main()