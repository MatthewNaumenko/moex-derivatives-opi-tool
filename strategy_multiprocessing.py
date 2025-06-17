import argparse
import time
import os
import psutil
from multiprocessing import Pool, Lock, Manager, current_process
from utils import fetch_and_save
from datetime import datetime, timedelta
import glob


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield (start_date + timedelta(n)).strftime('%Y%m%d')

def worker(args):
    date_str, csv_file, lock = args
    fetch_and_save(date_str, csv_file, lock=lock)


def main():
    parser = argparse.ArgumentParser(description='Параллельная загрузка данных с MOEX (multiprocessing.Pool)',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--start', required=True, help='Начальная дата (YYYYMMDD)')
    parser.add_argument('--end', required=True, help='Конечная дата (YYYYMMDD)')
    parser.add_argument('--csv', default='output_MIX.csv', help='Имя выходного CSV-файла')
    parser.add_argument('--proc', type=int, default=4, choices=[1,2,4,8,10,12,14], help='Число процессов (1-14)')
    parser.add_argument('--clean', action='store_true', help='Удалить все CSV-файлы в директории перед запуском')
    args = parser.parse_args()

    if args.clean:
        for f in glob.glob('*.csv'):
            try:
                os.remove(f)
                print(f'Удалён файл: {f}')
            except Exception as e:
                print(f'Ошибка при удалении {f}: {e}')
        print('Все CSV-файлы удалены.\n')

    start_dt = datetime.strptime(args.start, '%Y%m%d')
    end_dt = datetime.strptime(args.end, '%Y%m%d')
    all_dates = list(daterange(start_dt, end_dt))
    num_dates = len(all_dates)

    print(f'Обработка дат с {args.start} по {args.end} ({num_dates} дней) в {args.proc} процессах...')
    manager = Manager()
    lock = manager.Lock()

    cpu_percent_before = psutil.cpu_percent(interval=None)
    t0 = time.time()
    with Pool(processes=args.proc) as pool:
        pool.map(worker, [(date, args.csv, lock) for date in all_dates])
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