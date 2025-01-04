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
DATABASE_NAME = 'loty'


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
        'name': 'Get all airports with limit',
        'collection': 'airports',
        'operation': lambda collection: list(collection.find().limit(12))
    },
    {
        'name': 'Get all airlines with limit',
        'collection': 'airlines',
        'operation': lambda collection: list(collection.find().limit(12))
    },
    {
        'name': 'Get departure delays with limit',
        'collection': 'flights',
        'operation': lambda collection: list(collection.find({}, {'departure_delay': 1, '_id': 0}).limit(12))
    },
    {
        'name': 'Get US airports',
        'collection': 'airports',
        'operation': lambda collection: list(collection.find(
            {'country': 'United States'},
            {'airport': 1, 'city': 1, 'state': 1, '_id': 0}
        ).limit(10))
    },
    {
        'name': 'Average arrival delay by day of week',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$group': {
                '_id': '$day_of_week',
                'avg_arrival_delay': {'$avg': '$arrival_delay'}
            }},
            {'$sort': {'_id': 1}},
            {'$limit': 10}
        ],
    },
    {
        'name': 'Flights count by airline',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$group': {
                '_id': '$airline',
                'total_flights': {'$sum': 1}
            }},
            {'$sort': {'total_flights': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Average arrival delay by destination',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$group': {
                '_id': '$destination_airport',
                'avg_arrival_delay': {'$avg': '$arrival_delay'}
            }},
            {'$sort': {'avg_arrival_delay': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Cancelled flights with reasons',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$match': {'cancelled': 1}},
            {'$lookup': {
                'from': 'cancellation_codes',
                'localField': 'cancellation_reason',
                'foreignField': 'cancellation_reason',
                'as': 'cancellation_info'
            }},
            {'$unwind': '$cancellation_info'},
            {'$project': {
                'flight_number': 1,
                'destination_airport': 1,
                'cancellation_description': '$cancellation_info.cancellation_description'
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Flights with destination airport details',
        'collection': 'flights',
        'pipeline': [
            {'$lookup': {
                'from': 'airports',
                'localField': 'destination_airport',
                'foreignField': 'iata_code',
                'as': 'airport_info'
            }},
            {'$unwind': '$airport_info'},
            {'$project': {
                'flight_number': 1,
                'city': '$airport_info.city',
                'state': '$airport_info.state'
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Cancelled flights with codes',
        'collection': 'flights',
        'pipeline': [
            {'$lookup': {
                'from': 'cancellation_codes',
                'localField': 'cancellation_reason',
                'foreignField': 'cancellation_reason',
                'as': 'cancellation_info'
            }},
            {'$unwind': '$cancellation_info'},
            {'$project': {
                'cancellation_reason': 1,
                'flight_number': 1
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Flights with most common cancellation reason',
        'collection': 'flights',
        'pipeline': [
            {'$match': {'cancellation_reason': {'$ne': None}}},
            {'$group': {
                '_id': '$cancellation_reason',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 1},
            {'$lookup': {
                'from': 'flights',
                'let': {'common_reason': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$cancellation_reason', '$$common_reason']}
                    }},
                    {'$project': {
                        'flight_number': 1,
                        'destination_airport': 1,
                        'cancellation_reason': 1
                    }},
                    {'$limit': 10}
                ],
                'as': 'flights'
            }},
            {'$unwind': '$flights'},
            {'$replaceRoot': {'newRoot': '$flights'}}
        ]
    },
    {
        'name': 'Flights to high-delay airports',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$group': {
                '_id': '$destination_airport',
                'avg_delay': {'$avg': '$arrival_delay'}
            }},
            {'$match': {'avg_delay': {'$gt': 30}}},
            {'$lookup': {
                'from': 'flights',
                'let': {'high_delay_airport': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$destination_airport', '$$high_delay_airport']}
                    }},
                    {'$project': {
                        'flight_number': 1,
                        'destination_airport': 1,
                        'arrival_delay': 1
                    }},
                    {'$limit': 10}
                ],
                'as': 'flights'
            }},
            {'$unwind': '$flights'},
            {'$replaceRoot': {'newRoot': '$flights'}},
            {'$limit': 10}
        ],
    },
    {
        'name': 'Flights from airline with most flights',
        'collection': 'flights',
        'optimized_pipeline': [
            {'$group': {
                '_id': '$airline',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 1},
            {'$lookup': {
                'from': 'flights',
                'let': {'top_airline': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$airline', '$$top_airline']}
                    }},
                    {'$project': {
                        'flight_number': 1,
                        'destination_airport': 1,
                        'airline': 1
                    }},
                    {'$limit': 10}
                ],
                'as': 'flights'
            }},
            {'$unwind': '$flights'},
            {'$replaceRoot': {'newRoot': '$flights'}}
        ],
    }
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE FLIGHT (MongoDB)\n\n")

    print("Start of tests for the 'flight' database in MongoDB...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")

if __name__ == "__main__":
    main()