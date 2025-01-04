import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine

# Ustawienia połączenia
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'fizyka'
DATABASE = 'python_generator_data'
PORT = 5432

# Utwórz silnik SQLAlchemy
engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

def insert_data_in_batches(dataframe, batch_size):
    # Oblicz liczbę partii
    total_rows = dataframe.shape[0].compute()
    num_batches = (total_rows // batch_size) + 1

    for batch in range(num_batches):
        start = batch * batch_size
        end = start + batch_size
        
        # Pobierz partię i konwertuj do pandas DataFrame
        batch_df = dataframe.loc[start:end].compute()

        # Sprawdź, które appointment_id już istnieją w bazie
        existing_ids = pd.read_sql_query("SELECT appointment_id FROM appointments", con=engine)['appointment_id'].tolist()
        batch_df = batch_df[~batch_df['appointment_id'].isin(existing_ids)]

        # Wstaw dane do bazy, jeśli są nowe
        if not batch_df.empty:
            batch_df.to_sql('appointments', engine, if_exists='append', index=False)
            print(f"Wstawiono partię {batch + 1} z {num_batches}")
        else:
            print(f"Brak nowych danych do wstawienia w partii {batch + 1}.")

if __name__ == "__main__":
    # Załaduj dane z pliku CSV do Dask DataFrame
    csv_path = 'D:\\data_mining\\appointments.csv'  # Dostosuj ścieżkę
    data = dd.read_csv(csv_path)

    # Wstaw dane w partiach
    batch_size = 100  # Możesz dostosować rozmiar partii
    insert_data_in_batches(data, batch_size)
