import os
import time
import psutil
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv

# Wczytywanie zmiennych środowiskowe z pliku .env
load_dotenv()

# Ustawienia połączenia z bazą danych
DATABASE_CONFIG = {
    'dbname': 'trip',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

@contextmanager
def connect_to_db():
    """Context manager for database connection."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def measure_query_performance(query):
    """Executes a query and measures execution time, RAM, and CPU usage efficiently."""

    # Monitorowanie procesu dla RAM i CPU
    process = psutil.Process()

    # Próbka początkowa RAM-u i CPU
    ram_before = process.memory_info().rss / (1024 * 1024)  # RAM in MB
    cpu_before = psutil.cpu_percent(interval=None)

    # Start czasu wykonania zapytania
    start_time = time.perf_counter()

    # Wykonanie zapytania do bazy danych z próbkowaniem RAM i CPU w środku
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            # Próbka w środku zapytania
            mid_cpu = psutil.cpu_percent(interval=None)
            mid_ram = process.memory_info().rss / (1024 * 1024)  # RAM in MB
            cursor.fetchall()

    # Koniec czasu wykonania zapytania
    end_time = time.perf_counter()

    # Próbka końcowa RAM-u i CPU
    ram_after = process.memory_info().rss / (1024 * 1024)  # RAM in MB
    cpu_after = psutil.cpu_percent(interval=None)

    # Obliczenia wyników
    execution_time = end_time - start_time
    avg_ram = (ram_before + mid_ram + ram_after) / 3
    max_ram = max(ram_before, mid_ram, ram_after)
    avg_cpu = (cpu_before + mid_cpu + cpu_after) / 3
    max_cpu = max(cpu_before, mid_cpu, cpu_after)

    # Przygotowanie wyników do wypisania
    results = (
        f"Results for query:\n{query}\n"
        f"Completion time: {execution_time:.4f} s\n"
        f"Average RAM usage: {avg_ram:.4f} MB, Maximum RAM usage: {max_ram:.4f} MB\n"
        f"Average CPU performance: {avg_cpu:.4f}%, Maximum CPU performance: {max_cpu:.4f}%\n"
    )

    # Wyświetlenie wyników na konsoli
    print(results)

    # Zapisanie wyników do pliku
    with open("result.txt", "a") as f:  # 'a' oznacza tryb dopisywania
        f.write(results + "\n")

# Przykładowe zapytania
queries = [
    "SELECT * FROM users LIMIT 10;",
    "SELECT station_name, latitude, longitude FROM stations LIMIT 10;",
    "SELECT * FROM trips WHERE tripduration > 1800 LIMIT 10;",
    "SELECT gender, COUNT(*) AS trip_count FROM trips t JOIN users u ON t.user_id = u.user_id GROUP BY gender LIMIT 10;",
    "SELECT user_id, AVG(tripduration) AS average_tripduration FROM trips GROUP BY user_id LIMIT 10;",
    "SELECT start_station_id, COUNT(*) AS trips_started FROM trips GROUP BY start_station_id LIMIT 10;",
    "SELECT * FROM trips WHERE start_station_id = (SELECT start_station_id FROM trips GROUP BY start_station_id ORDER BY COUNT(*) DESC LIMIT 1) LIMIT 10;",
    "SELECT * FROM users WHERE user_id IN (SELECT user_id FROM trips WHERE start_station_id = (SELECT station_id FROM stations ORDER BY SQRT(POW(latitude, 2) + POW(longitude, 2)) DESC LIMIT 1)) LIMIT 10;",
    "SELECT * FROM trips WHERE tripduration > (SELECT AVG(tripduration) FROM trips) LIMIT 10;",
    "SELECT u.user_id, u.birth_year, u.gender, s.station_name AS start_station FROM trips t JOIN users u ON t.user_id = u.user_id JOIN stations s ON t.start_station_id = s.station_id LIMIT 10;",
    "SELECT t.trip_id, s1.station_name AS start_station, s2.station_name AS end_station FROM trips t JOIN stations s1 ON t.start_station_id = s1.station_id JOIN stations s2 ON t.end_station_id = s2.station_id LIMIT 10",
    "SELECT t.trip_id, t.tripduration, 2024 - u.birth_year AS user_age FROM trips t JOIN users u ON t.user_id = u.user_id LIMIT 10;",
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE TRIP\n\n")

    print("Start of tests for the 'trip' database...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")
if __name__ == "__main__":
    main()
