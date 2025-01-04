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
DATABASE_NAME = 'trip'


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
        'name': 'Get all users',
        'collection': 'users',
        'operation': lambda collection: list(collection.find({}).limit(10))
    },
    {
        'name': 'Get station coordinates',
        'collection': 'stations',
        'operation': lambda collection: list(collection.find(
            {},
            {'station_name': 1, 'latitude': 1, 'longitude': 1, '_id': 0}
        ).limit(10))
    },
    {
        'name': 'Get long trips',
        'collection': 'trips',
        'operation': lambda collection: list(collection.find(
            {'tripduration': {'$gt': 1800}}
        ).limit(10))
    },
    {
        'name': 'Trips count by gender',
        'collection': 'trips',
        'pipeline': [
            # Najpierw grupujemy po user_id aby zmniejszyć liczbę dokumentów przed lookup
            {'$group': {
                '_id': '$user_id',
                'trips': {'$sum': 1}
            }},
            # Następnie wykonujemy lookup na mniejszym zestawie danych
            {'$lookup': {
                'from': 'users',
                'localField': '_id',

                'foreignField': 'user_id',
                'as': 'user'
            }},
            {'$unwind': '$user'},
            # Końcowe grupowanie po płci
            {'$group': {
                '_id': '$user.gender',
                'trip_count': {'$sum': '$trips'}
            }},
            {'$sort': {'trip_count': -1}},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Average trip duration by user',
        'collection': 'trips',
        'pipeline': [
            {'$group': {
                '_id': '$user_id',
                'average_tripduration': {'$avg': '$tripduration'}
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Trips count by start station',
        'collection': 'trips',
        'pipeline': [
            {'$group': {
                '_id': '$start_station_id',
                'trips_started': {'$sum': 1}
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Trips from most popular station',
        'collection': 'trips',
        'pipeline': [
            {'$group': {
                '_id': '$start_station_id',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 1},
            {'$lookup': {
                'from': 'trips',
                'let': {'station_id': '$_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$start_station_id', '$$station_id']}
                    }},
                    {'$limit': 10}
                ],
                'as': 'trips'
            }},
            {'$unwind': '$trips'},
            {'$replaceRoot': {'newRoot': '$trips'}}
        ]
    },
    {
        'name': 'Users starting from farthest station',
        'collection': 'stations',
        'pipeline': [
            {'$addFields': {
                'distance': {
                    '$sqrt': {
                        '$add': [
                            {'$pow': ['$latitude', 2]},
                            {'$pow': ['$longitude', 2]}
                        ]
                    }
                }
            }},
            {'$sort': {'distance': -1}},
            {'$limit': 1},
            {'$lookup': {
                'from': 'trips',
                'let': {'station_id': '$station_id'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$eq': ['$start_station_id', '$$station_id']}
                    }},
                    {'$lookup': {
                        'from': 'users',
                        'localField': 'user_id',
                        'foreignField': 'user_id',
                        'as': 'user'
                    }},
                    {'$unwind': '$user'},
                    {'$replaceRoot': {'newRoot': '$user'}},
                    {'$limit': 10}
                ],
                'as': 'users'
            }},
            {'$unwind': '$users'},
            {'$replaceRoot': {'newRoot': '$users'}}
        ]
    },
    {
        'name': 'Trips longer than average',
        'collection': 'trips',
        'pipeline': [
            {'$group': {
                '_id': None,
                'avg_duration': {'$avg': '$tripduration'}
            }},
            {'$lookup': {
                'from': 'trips',
                'let': {'avg_duration': '$avg_duration'},
                'pipeline': [
                    {'$match': {
                        '$expr': {'$gt': ['$tripduration', '$$avg_duration']}
                    }},
                    {'$limit': 10}
                ],
                'as': 'trips'
            }},
            {'$unwind': '$trips'},
            {'$replaceRoot': {'newRoot': '$trips'}}
        ]
    },
    {
        'name': 'User trip start stations',
        'collection': 'trips',
        'pipeline': [
            {'$lookup': {
                'from': 'users',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'user'
            }},
            {'$lookup': {
                'from': 'stations',
                'localField': 'start_station_id',
                'foreignField': 'station_id',
                'as': 'station'
            }},
            {'$unwind': '$user'},
            {'$unwind': '$station'},
            {'$project': {
                'user_id': '$user.user_id',
                'birth_year': '$user.birth_year',
                'gender': '$user.gender',
                'start_station': '$station.station_name'
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Trip stations',
        'collection': 'trips',
        'pipeline': [
            {'$lookup': {
                'from': 'stations',
                'localField': 'start_station_id',
                'foreignField': 'station_id',
                'as': 'start_station'
            }},
            {'$lookup': {
                'from': 'stations',
                'localField': 'end_station_id',
                'foreignField': 'station_id',
                'as': 'end_station'
            }},
            {'$unwind': '$start_station'},
            {'$unwind': '$end_station'},
            {'$project': {
                'trip_id': 1,
                'start_station': '$start_station.station_name',
                'end_station': '$end_station.station_name'
            }},
            {'$limit': 10}
        ]
    },
    {
        'name': 'Trip durations with user ages',
        'collection': 'trips',
        'pipeline': [
            {'$lookup': {
                'from': 'users',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'user'
            }},
            {'$unwind': '$user'},
            {'$project': {
                'trip_id': 1,
                'tripduration': 1,
                'user_age': {'$subtract': [2024, '$user.birth_year']}
            }},
            {'$limit': 10}
        ]
    }
]

def main():
    with open("result.txt", "a") as f:
        f.write("DATABASE TRIP (MongoDB)\n\n")

    print("Start of tests for the 'trip' database in MongoDB...\n")
    for query in queries:
        measure_query_performance(query)

    with open("result.txt", "a") as f:
        f.write("==========\n")

if __name__ == "__main__":
    main()