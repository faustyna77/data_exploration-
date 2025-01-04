import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.exc import SQLAlchemyError

# Połączenie do bazy danych PostgreSQL
username = 'postgres'
password = 'fizyka'
host = 'localhost'  # lub inny host
port = '5432'  # domyślny port PostgreSQL
database = 'python_generator_data'
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
Base = declarative_base()

# Definicje tabel

# Tabela Doctors

class Doctor(Base):
    __tablename__ = 'doctors'
    doctor_id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    last_name = Column(String(50))
    email = Column(String(100))
    specialization = Column(String(50))
    
    # Relacja z Appointment
    appointments = relationship("Appointment", back_populates="doctor")

# Tabela Patients
class Patient(Base):
    __tablename__ = 'patients'
    patient_id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    last_name = Column(String(50))
    birthdate = Column(Date)
    phone_number = Column(String(50))
    
    # Relacja z Appointment
    appointments = relationship("Appointment", back_populates="patient")

# Tabela Appointments
class Appointment(Base):
    __tablename__ = 'appointments'
    appointment_id = Column(Integer, primary_key=True)
    doctor_id = Column(Integer, ForeignKey('doctors.doctor_id'))  # Klucz obcy do Doctors
    patient_id = Column(Integer, ForeignKey('patients.patient_id'))  # Klucz obcy do Patients
    appointment_date = Column(Date)
    diagnosis = Column(String(100))
    treatment = Column(String(100))
    
    # Relacje do tabel Doctors i Patients
    doctor = relationship("Doctor", back_populates="appointments")
    patient = relationship("Patient", back_populates="appointments")

# Tworzenie tabel w bazie danych
Base.metadata.create_all(engine)

# Ścieżki do plików CSV
doctors_csv_path = 'D:\\data_mining\\doctors.csv'
patients_csv_path = 'D:\\data_mining\\patients.csv'
appointments_csv_path = 'D:\\data_mining\\appointments.csv'

# Funkcja do importu danych
def import_data_to_table(csv_path, table_name):
    try:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Dane zostały zaimportowane do tabeli '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"Wystąpił błąd podczas importowania danych do tabeli '{table_name}': {e}")

# Import danych do każdej tabeli
import_data_to_table(doctors_csv_path, 'doctors')
import_data_to_table(patients_csv_path, 'patients')
import_data_to_table(appointments_csv_path, 'appointments')

# Zamknięcie połączenia
engine.dispose()
