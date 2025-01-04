# -*- coding: utf-8 -*-

import os
import time
import psutil
from pymongo import MongoClient
from contextlib import contextmanager
from dotenv import load_dotenv

# Wczytywanie zmiennych środowiskowych z pliku .env
load_dotenv()

# Uproszczona konfiguracja połączenia z MongoDB
MONGODB_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = 'przychodnia'


@contextmanager
def connect_to_mongodb():
    """Context manager dla połączenia z MongoDB."""
    client = MongoClient(MONGODB_URI)
    try:
        db = client[DATABASE_NAME]
        yield db
    finally:
        client.close()


def measure_query_performance(query):
    """Wykonuje zapytanie MongoDB i mierzy czas wykonania, użycie RAM i CPU."""

    # Monitorowanie procesu dla RAM i CPU
    process = psutil.Process()

    # Próbka początkowa RAM-u i CPU
    ram_before = process.memory_info().rss / (1024 * 1024)
    cpu_before = psutil.cpu_percent(interval=None)

    # Start czasu wykonania zapytania
    start_time = time.perf_counter()

    # Wykonanie zapytania do bazy danych
    with connect_to_mongodb() as db:
        collection = db[query['collection']]

        # Wykonanie odpowiedniego typu zapytania
        if 'pipeline' in query:
            # Wykonanie pipeline agregacji
            list(collection.aggregate(query['pipeline']))
        elif 'operation' in query:
            # Wykonanie własnej operacji
            query['operation'](collection)

        # Próbka w środku zapytania
        mid_cpu = psutil.cpu_percent(interval=None)
        mid_ram = process.memory_info().rss / (1024 * 1024)

    # Koniec czasu wykonania zapytania
    end_time = time.perf_counter()

    # Próbka końcowa RAM-u i CPU
    ram_after = process.memory_info().rss / (1024 * 1024)
    cpu_after = psutil.cpu_percent(interval=None)

    # Obliczenia wyników
    execution_time = end_time - start_time
    avg_ram = (ram_before + mid_ram + ram_after) / 3
    max_ram = max(ram_before, mid_ram, ram_after)
    avg_cpu = (cpu_before + mid_cpu + cpu_after) / 3
    max_cpu = max(cpu_before, mid_cpu, cpu_after)

    # Przygotowanie wyników do wypisania
    results = (
        f"Results for query: {query['name']}\n"
        f"Completion time: {execution_time:.4f} s\n"
        f"Average RAM usage: {avg_ram:.4f} MB, Maximum RAM usage: {max_ram:.4f} MB\n"
        f"Average CPU performance: {avg_cpu:.4f}%, Maximum CPU performance: {max_cpu:.4f}%\n"
    )

    # Wyświetlenie wyników na konsoli
    print(results)

    # Zapisanie wyników do pliku
    with open("result.txt", "a") as f:
        f.write(results + "\n")


# Przykładowe zapytania
queries = [
    {
        'name': 'Get pediatrics doctors',
        'collection': 'doctors',
        'operation': lambda collection: list(collection.find(
            {'specialization': 'Pediatrics'},
            {'first_name': 1, 'last_name': 1, 'specialization': 1, '_id': 0}
        ).limit(10))
    },
    {
        'name': 'Get patients born before 2000',
        'collection': 'patients',
        'operation': lambda collection: list(collection.find(
            {'birthdate': {'$lt': '2000-01-01'}},
            {'first_name': 1, 'last_name': 1, 'birthdate': 1, '_id': 0}
        ).limit(10))
    },
    {
        'name': 'Get appointments info',
        'collection': 'appointments',
        'operation': lambda collection: list(collection.find(
            {},
            {'appointment_date': 1, 'diagnosis': 1, '_id': 0}
        ).limit(10))
    },
    {
        'name': 'Doctors by total patients',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$doctor_id',
                'total_patients': {'$sum': 1}
            }},
            {'$sort': {'total_patients': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Patients by total appointments',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$patient_id',
                'total_appointments': {'$sum': 1}
            }},
            {'$sort': {'total_appointments': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Appointments count by date',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$appointment_date',
                'total_appointments': {'$sum': 1}
            }},
            {'$sort': {'total_appointments': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Detailed appointments info',
        'collection': 'appointments',
        'pipeline': [
            {'$lookup': {
                'from': 'doctors',
                'localField': 'doctor_id',
                'foreignField': 'doctor_id',
                'as': 'doctor'
            }},
            {'$lookup': {
                'from': 'patients',
                'localField': 'patient_id',
                'foreignField': 'patient_id',
                'as': 'patient'
            }},
            {'$unwind': '$doctor'},
            {'$unwind': '$patient'},
            {'$project': {
                'appointment_date': 1,
                'doctor_first_name': '$doctor.first_name',
                'doctor_last_name': '$doctor.last_name',
                'patient_first_name': '$patient.first_name',
                'patient_last_name': '$patient.last_name',
                'diagnosis': 1
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Patients with more than 5 appointments',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$patient_id',
                'total_appointments': {'$sum': 1}
            }},
            {'$match': {'total_appointments': {'$gt': 5}}},
            {'$lookup': {
                'from': 'patients',
                'localField': '_id',
                'foreignField': 'patient_id',
                'as': 'patient_info'
            }},
            {'$unwind': '$patient_info'},
            {'$project': {
                'first_name': '$patient_info.first_name',
                'last_name': '$patient_info.last_name',
                'total_appointments': 1
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Doctors who diagnosed Flu',
        'collection': 'appointments',
        'pipeline': [
            {'$match': {'diagnosis': 'Flu'}},
            {'$lookup': {
                'from': 'doctors',
                'localField': 'doctor_id',
                'foreignField': 'doctor_id',
                'as': 'doctor'
            }},
            {'$unwind': '$doctor'},
            {'$group': {
                '_id': {
                    'first_name': '$doctor.first_name',
                    'last_name': '$doctor.last_name'
                }
            }},
            {'$project': {
                'first_name': '$_id.first_name',
                'last_name': '$_id.last_name',
                '_id': 0
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Patients with multiple appointments',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$patient_id',
                'appointment_count': {'$sum': 1}
            }},
            {'$match': {'appointment_count': {'$gt': 1}}},
            {'$lookup': {
                'from': 'appointments',
                'let': {'patient_id': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$patient_id', '$$patient_id']}
                    }},
                    {'$lookup': {
                        'from': 'patients',
                        'localField': 'patient_id',
                        'foreignField': 'patient_id',
                        'as': 'patient'
                    }},
                    {'$unwind': '$patient'},
                    {'$project': {
                        'appointment_id': 1,
                        'appointment_date': 1,
                        'first_name': '$patient.first_name',
                        'last_name': '$patient.last_name',
                        'diagnosis': 1
                    }}
                ],
                'as': 'appointments'
            }},
            {'$unwind': '$appointments'},
            {'$replaceRoot': {'newRoot': '$appointments'}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Doctors with elderly patients',
        'collection': 'appointments',
        'pipeline': [
            {'$lookup': {
                'from': 'patients',
                'localField': 'patient_id',
                'foreignField': 'patient_id',
                'as': 'patient'
            }},
            {'$unwind': '$patient'},
            {'$match': {'patient.birthdate': {'$lt': '1980-01-01'}}},
            {'$lookup': {
                'from': 'doctors',
                'localField': 'doctor_id',
                'foreignField': 'doctor_id',
                'as': 'doctor'
            }},
            {'$unwind': '$doctor'},
            {'$group': {
                '_id': {
                    'first_name': '$doctor.first_name',
                    'last_name': '$doctor.last_name'
                }
            }},
            {'$project': {
                'first_name': '$_id.first_name',
                'last_name': '$_id.last_name',
                '_id': 0
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Patients of busiest doctor',
        'collection': 'appointments',
        'pipeline': [
            {'$group': {
                '_id': '$doctor_id',
                'patient_count': {'$sum': 1}
            }},
            {'$sort': {'patient_count': -1}},
            {'$limit': 1},
            {'$lookup': {
                'from': 'appointments',
                'let': {'doctor_id': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$doctor_id', '$$doctor_id']}
                    }},
                    {'$lookup': {
                        'from': 'patients',
                        'localField': 'patient_id',
                        'foreignField': 'patient_id',
                        'as': 'patient'
                    }},
                    {'$unwind': '$patient'},
                    {'$group': {
                        '_id': {
                            'patient_id': '$patient_id',
                            'first_name': '$patient.first_name',
                            'last_name': '$patient.last_name'
                        }
                    }},
                    {'$project': {
                        'first_name': '$_id.first_name',
                        'last_name': '$_id.last_name',
                        '_id': 0
                    }},
                    {'$limit': 10}
                ],
                'as': 'patients'
            }},
            {'$unwind': '$patients'},
            {'$replaceRoot': {'newRoot': '$patients'}}
        ]
    }
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE CLINIC (MongoDB)\n\n")

    print("Start of tests for the 'przychodnia' database in MongoDB...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")

if __name__ == "__main__":
    main()