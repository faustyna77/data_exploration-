
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app)  # Dodaj CORS dla wszystkich endpointów

# Funkcja do wczytywania danych z Excela
def load_excel_data():
    df = pd.read_excel('compare_databases.xlsx')
    df['source'] = df['Baza danych'].apply(lambda x: 'MongoDB' if 'MongoDB' in str(x) else 'PostgreSQL')
    df['Zapytanie'] = df['Zapytanie'].fillna("N/A")  # Upewniamy się, że brakujące wartości są uzupełnione
    postgresql_data = df[df['source'] == 'PostgreSQL']
    mongodb_data = df[df['source'] == 'MongoDB']
    return postgresql_data, mongodb_data

@app.route('/compare', methods=['GET'])
def compare():
    postgresql_data, mongodb_data = load_excel_data()
    
    # Dodajemy 'Zapytanie' do MongoDB
    response = {
        'PostgreSQL': postgresql_data[['Baza danych', 'Czas wykonania (s)', 'Maksymalna wydajność CPU (%)', 
                                       'Maksymalne zużycie RAM (MB)', 'Zapytanie', 
                                       'Średnia wydajność CPU (%)', 'Średnie zużycie RAM (MB)']].to_dict(orient='records'),
        
        'MongoDB': mongodb_data[['Baza danych', 'Czas wykonania (s)', 'Maksymalna wydajność CPU (%)', 
                                 'Maksymalne zużycie RAM (MB)', 'Zapytanie',  # Dodajemy kolumnę 'Zapytanie'
                                 'Średnia wydajność CPU (%)', 'Średnie zużycie RAM (MB)']].to_dict(orient='records')
    }
    
    return jsonify(response)

if __name__ == '__main__':
    
    app.run(host="0.0.0.0", port=5000)