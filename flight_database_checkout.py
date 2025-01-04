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
    'dbname': 'loty',
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
    "SELECT * FROM airports LIMIT 12;",
    "SELECT * FROM airlines LIMIT 12;",
    "SELECT departure_delay FROM flights LIMIT 12;",
    "SELECT airport, city, state FROM airports WHERE country = 'United States' LIMIT 10;",
    "SELECT day_of_week, AVG(arrival_delay) AS avg_arrival_delay FROM flights GROUP BY day_of_week ORDER BY day_of_week LIMIT 10;",
    "SELECT airline, COUNT(*) AS total_flights FROM flights GROUP BY airline ORDER BY total_flights DESC LIMIT 10;",
    "SELECT destination_airport, AVG(arrival_delay) AS avg_arrival_delay FROM flights GROUP BY destination_airport ORDER BY avg_arrival_delay DESC LIMIT 10;",
    "SELECT f.flight_number, f.destination_airport, f.destination_airport, c.cancellation_description FROM flights f JOIN cancellation_codes c ON f.cancellation_reason = c.cancellation_reason WHERE f.cancelled = 1 LIMIT 10;",
    "SELECT f.flight_number, a.city, a.state FROM flights f JOIN airports a ON f.destination_airport = a.iata_code LIMIT 10;",
    "SELECT cancellation_codes.cancellation_reason,flights.flight_number FROM cancellation_codes JOIN flights ON cancellation_codes.cancellation_reason=cancellation_codes.cancellation_reason LIMIT 10;",
    "SELECT f.flight_number, f.destination_airport, f.destination_airport, f.cancellation_reason FROM flights f WHERE f.cancellation_reason = ( SELECT cancellation_reason FROM flights WHERE cancellation_reason IS NOT NULL GROUP BY cancellation_reason ORDER BY COUNT(*) DESC LIMIT 1)LIMIT 10;",
    "SELECT flight_number, destination_airport, destination_airport, arrival_delay FROM flights WHERE destination_airport IN (SELECT destination_airport FROM flights GROUP BY destination_airport HAVING AVG(arrival_delay) > 30) LIMIT 10;",
    "SELECT flight_number, destination_airport, destination_airport, airline FROM flights WHERE airline = (SELECT airline FROM flights GROUP BY airline ORDER BY COUNT(*) DESC LIMIT 1) LIMIT 10;",
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE FLIGHT\n\n")

    print("Start of tests for the 'flight' database...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")
if __name__ == "__main__":
    main()
