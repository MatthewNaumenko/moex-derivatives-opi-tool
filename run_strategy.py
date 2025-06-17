import subprocess
import time
import sys
from tabulate import tabulate

# Настройки диапазона дат и базовых имён файлов
start_date = '20250408'
end_date = '20250501'
proc_list = [1, 2, 4, 6, 8, 10, 12, 14]
repeats = 3

strategies = [
    {
        'name': 'multiprocessing',
        'cmd': lambda p, run: [sys.executable, 'strategy_multiprocessing.py', '--start', start_date, '--end', end_date, '--csv', f'output_MIX_mp_{p}_{run}.csv', '--proc', str(p), '--clean'],
        'csv': 'output_MIX_mp',
    },
    {
        'name': 'mpi',
        'cmd': lambda p, run: ['mpiexec', '-n', str(p), sys.executable, 'strategy_mpi.py', '--start', start_date, '--end', end_date, '--csv', f'output_MIX_mpi_{p}_{run}.csv', '--clean'],
        'csv': 'output_MIX_mpi',
    },
    {
        'name': 'async',
        'cmd': lambda p, run: [sys.executable, 'strategy_async.py', '--start', start_date, '--end', end_date, '--csv', f'output_MIX_async_{p}_{run}.csv', '--proc', str(p), '--concurrent', str(p), '--clean'],
        'csv': 'output_MIX_async',
    },
    # TODO: future strategy
]

results = []

for strat in strategies:
    for p in proc_list:
        for run in range(1, repeats+1):
            print(f"\nRunning {strat['name']} | processes: {p} | run: {run}")
            cmd = strat['cmd'](p, run)
            try:
                start = time.time()
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                end = time.time()
                output = proc.stdout + proc.stderr
                # Парсим время и throughput из вывода
                exec_time = None
                throughput = None
                for line in output.splitlines():
                    if 'Time elapsed:' in line:
                        exec_time = float(line.split(':')[1].split()[0])
                    if 'Average throughput:' in line:
                        throughput = float(line.split(':')[1].split()[0])
                results.append({
                    'Method': strat['name'],
                    'Processes': p,
                    'Run': run,
                    'ExecTime': exec_time if exec_time is not None else round(end-start,2),
                    'Throughput': throughput if throughput is not None else '-',
                })
                print(f"Done: {strat['name']} | proc: {p} | run: {run} | Time: {exec_time} | Throughput: {throughput}")
            except Exception as e:
                print(f"Error running {strat['name']} with {p} processes, run {run}: {e}")
                results.append({
                    'Method': strat['name'],
                    'Processes': p,
                    'Run': run,
                    'ExecTime': '-',
                    'Throughput': '-',
                })

# Выводим таблицу
print("\n==== Benchmark Results ====")
print(tabulate(results, headers="keys", tablefmt="github"))