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
    'dbname': 'przychodnia',
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
    "SELECT first_name, last_name, specialization FROM doctors WHERE specialization = 'Pediatrics' LIMIT 10;",
    "SELECT first_name, last_name, birthdate FROM patients WHERE birthdate < '2000-01-01' LIMIT 10;",
    "SELECT appointment_date, diagnosis FROM appointments LIMIT 10;",
    "SELECT doctor_id, COUNT(*) AS total_patients FROM appointments GROUP BY doctor_id ORDER BY total_patients DESC LIMIT 10;",
    "SELECT patient_id, COUNT(*) AS total_appointments FROM appointments GROUP BY patient_id ORDER BY total_appointments DESC LIMIT 10;",
    "SELECT appointment_date, COUNT(*) AS total_appointments FROM appointments GROUP BY appointment_date ORDER BY total_appointments DESC LIMIT 10;",
    "SELECT a.appointment_date, d.first_name AS doctor_first_name, d.last_name AS doctor_last_name, p.first_name AS patient_first_name, p.last_name AS patient_last_name, a.diagnosis FROM appointments a JOIN doctors d ON a.doctor_id = d.doctor_id JOIN patients p ON a.patient_id = p.patient_id LIMIT 10;",
    "SELECT p.first_name, p.last_name, COUNT(a.appointment_id) AS total_appointments FROM patients p JOIN appointments a ON p.patient_id = a.patient_id GROUP BY p.patient_id HAVING COUNT(a.appointment_id) > 5 LIMIT 10;",
    "SELECT DISTINCT d.first_name, d.last_name FROM doctors d JOIN appointments a ON d.doctor_id = a.doctor_id WHERE a.diagnosis = 'Flu' LIMIT 10;",
    "SELECT a.appointment_id, a.appointment_date, p.first_name, p.last_name, a.diagnosis FROM appointments a JOIN patients p ON a.patient_id = p.patient_id WHERE a.patient_id IN (SELECT patient_id FROM appointments GROUP BY patient_id HAVING COUNT(appointment_id) > 1) LIMIT 10;",
    "SELECT DISTINCT d.first_name, d.last_name FROM doctors d JOIN appointments a ON d.doctor_id = a.doctor_id JOIN patients p ON a.patient_id = p.patient_id WHERE p.birthdate < '1980-01-01' LIMIT 10;",
    "SELECT p.first_name, p.last_name FROM patients p WHERE p.patient_id IN (SELECT patient_id FROM appointments WHERE doctor_id = (SELECT doctor_id FROM appointments GROUP BY doctor_id ORDER BY COUNT(*) DESC LIMIT 1)) LIMIT 10;",
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE CLINIC\n\n")

    print("Start of tests for the 'przychodnia' database...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")
if __name__ == "__main__":
    main()
