# MOEX Open Positions Data Collector

## Описание

**moex-derivatives-opi-tool** — это инструмент для массовой и быстрой загрузки, парсинга и сохранения данных по открытым позициям на срочном рынке Московской биржи (MOEX). Проект реализует несколько стратегий параллельной и асинхронной обработки, что позволяет сравнивать производительность разных подходов (multiprocessing, MPI, asyncio).

- Автоматизация сбора исторических данных по фьючерсам на индекс МосБиржи
- Параллельная и асинхронная обработка для ускорения загрузки
- Гибкая настройка диапазона дат, числа процессов, стратегии
- Сравнение производительности разных подходов

---

## Структура проекта

```
Portfolytics/
├── run_strategy.py              # Бенчмаркинг: автоматический запуск и сравнение стратегий
├── strategy_multiprocessing.py  # Параллельная загрузка через multiprocessing.Pool
├── strategy_mpi.py              # Параллельная загрузка через MPI (mpi4py)
├── strategy_async.py            # Асинхронная загрузка через asyncio + ProcessPool
├── utils.py                     # Вспомогательные функции: парсинг, запросы, запись CSV
├── .gitignore                   # Исключения для git
└── README.md                    # Описание проекта
```

---

## Требования

- Python 3.8+
- [requests](https://pypi.org/project/requests/)
- [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)
- [psutil](https://pypi.org/project/psutil/)
- [tabulate](https://pypi.org/project/tabulate/)
- [aiohttp](https://pypi.org/project/aiohttp/)
- [mpi4py](https://pypi.org/project/mpi4py/) (для MPI-стратегии)

Установить зависимости:
```bash
pip install -r requirements.txt
```

---

## Быстрый старт

1. **Клонируйте репозиторий:**
```bash
git clone ...
cd Portfolytics
```
2. **Установите зависимости** (см. выше)
3. **Запустите нужную стратегию:**

### Примеры запуска

#### Multiprocessing
```bash
python strategy_multiprocessing.py --start 20250408 --end 20250501 --csv output_MIX_mp.csv --proc 8 --clean
```

#### MPI (mpi4py)
```bash
mpiexec -n 4 python strategy_mpi.py --start 20250408 --end 20250501 --csv output_MIX_mpi.csv --clean
```

#### Async + ProcessPool
```bash
python strategy_async.py --start 20250408 --end 20250501 --csv output_MIX_async.csv --proc 8 --concurrent 8 --clean
```

#### Бенчмаркинг всех стратегий
```bash
python run_strategy.py
```

---

## Описание стратегий

- **strategy_multiprocessing.py** — параллельная загрузка с помощью multiprocessing.Pool. Каждый процесс скачивает и парсит свою дату.
- **strategy_mpi.py** — параллельная загрузка с помощью MPI (mpi4py). Даты делятся между процессами по рангу.
- **strategy_async.py** — асинхронная загрузка HTML через aiohttp, парсинг и запись в CSV через пул процессов.
- **run_strategy.py** — автоматический запуск всех стратегий с разными параметрами, сбор и вывод статистики.
- **utils.py** — функции для парсинга HTML, формирования запросов, записи в CSV, преобразования дат.

---

## Результаты

- Все результаты сохраняются в CSV-файлы (по умолчанию игнорируются git).
- В конце работы выводится статистика: время, throughput, загрузка CPU.

---

## Лицензия

GNU Affero General Public License v3.0 (AGPL-3.0)
