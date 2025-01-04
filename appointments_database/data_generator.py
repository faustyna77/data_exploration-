from faker import Faker
import pandas as pd
import random

fake = Faker()

# Lista przykładowych domen
domains = ['example.com', 'hospital.com', 'medclinic.org', 'healthcare.net']


# Generowanie lekarzy z emailami opartymi o imię i nazwisk
def generate_doctors(n):
    doctors = []
    specializations = ['Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Dermatology']

    for i in range(1, n + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Tworzenie emaila na podstawie imienia, nazwiska i losowej domeny
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

        doctors.append({
            'doctor_id': i,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'specialization': random.choice(specializations)
        })

    return doctors


# Generowanie pacjentów
def generate_patients(n):
    patients = []
    for i in range(1, n + 1):
        patients.append({
            'patient_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birthdate': fake.date_of_birth(minimum_age=0, maximum_age=90),
            'phone_number': fake.phone_number()
        })
    return patients


# Generowanie wizyt pacjentów u lekarzy
def generate_appointments(n, doctor_ids, patient_ids):
    diagnoses = [
        'Hypertension', 'Type 2 diabetes', 'Cold', 'Pneumonia',
        'Asthma', 'Migraine', 'Urinary tract infection', 'Depression', 'Rheumatoid arthritis',
        'Sleep disorders', 'Heart failure', 'Alzheimers disease', 'Bronchitis'    
    ]

    treatments = [
        'Pharmacological treatment', 'Physiotherapy', 'Psychological consultation',
        'Surgery', 'Antibiotics', 'Anti-inflammatory drugs', 'Breathing exercises',
        'Low-sodium diet', 'Painkillers', 'Antihistamines'
    ]

    appointments = []
    for i in range(1, n + 1):
        doctor_id = random.choice(doctor_ids)
        patient_id = random.choice(patient_ids)
        appointments.append({
            'appointment_id': i,
            'doctor_id': doctor_id,
            'patient_id': patient_id,
            'appointment_date': fake.date_this_year(),
            'diagnosis': random.choice(diagnoses),
            'treatment': random.choice(treatments)
        })
    return appointments


# Zapis danych do plików CSV
def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


# Główna funkcja do generowania danych
def generate_database(num_doctors, num_patients, num_appointments):
    doctors = generate_doctors(num_doctors)
    patients = generate_patients(num_patients)
    appointments = generate_appointments(num_appointments, [d['doctor_id'] for d in doctors],
                                         [p['patient_id'] for p in patients])

    # Zapisanie danych do plików CSV
    save_to_csv(doctors, 'doctors.csv')
    save_to_csv(patients, 'patients.csv')
    save_to_csv(appointments, 'appointments.csv')


if __name__ == "__main__":
    num_doctors = int(input("How many doctors to generate? "))
    num_patients = int(input("How many patients to generate? "))
    num_appointments = int(input("How many appointments to generate? "))

    generate_database(num_doctors, num_patients, num_appointments)
    print("Data was generated and saved to CSV files.")