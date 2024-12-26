import requests
import time
import csv
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_name = os.getenv("DB")

# Base URL for Bitvavo API
BITVAVO_API_URL = 'https://api.bitvavo.com/v2/ticker/price'

# Log file setup
LOG_FILE = 'script.log'

def log_message(message):
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# MySQL database setup
DB_CONFIG = {
    'host': db_host,
    'user': db_user,
    'password': db_pass,
    'database': db_name
}

def setup_database():
    connection = None  # Initialize the connection variable
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        connection.database = DB_CONFIG['database']

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                market VARCHAR(50),
                price DECIMAL(18, 8)
            )
        """)
        log_message("Database setup completed successfully.")
    except Error as e:
        log_message(f"Error during database setup: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Function to fetch data from Bitvavo API
def fetch_data():
    retries = 0  # Counter for retries
    while retries <= 10:  # Retry up to 10 times
        try:
            response = requests.get(BITVAVO_API_URL)
            if response.status_code == 200:
                data = response.json()
                log_message("Successfully fetched data from Bitvavo API.")
                return data
            else:
                log_message(f"Failed to fetch data, status code: {response.status_code}")
        except Exception as e:
            log_message(f"Error occurred: {e}")

        retries += 1
        time.sleep(1)

    log_message("Failed to fetch data after 10 retries.")
    return None

# Function to fetch exchange rates directly from Bitvavo API
def fetch_exchange_rate():
    try:
        response = requests.get(BITVAVO_API_URL)
        if response.status_code == 200:
            data = response.json()
            for item in data:
                if item['market'] == 'USDC-EUR':
                    euro_price = float(item['price'])
                    log_message(f"Successfully fetched exchange rate from Bitvavo API: {euro_price}.")
                    return euro_price
        else:
            log_message(f"Failed to fetch exchange rate, status code: {response.status_code}")
    except Exception as e:
        log_message(f"Error occurred while fetching exchange rate: {e}")
    return None

# Function to save data to MySQL
def save_to_database(data):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        for item in data:
            cursor.execute(
                "INSERT INTO Prices (timestamp, market, price) VALUES (%s, %s, %s)",
                (item['timestamp'], item['market'], item['price'])
            )
        connection.commit()
        log_message("Successfully inserted data into the database.")
    except Error as e:
        log_message(f"Error while inserting data into database: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Main loop to fetch data every 10 seconds and save with timestamps
def main():
    setup_database()

    usd_to_eur_rate = fetch_exchange_rate()
    if not usd_to_eur_rate:
        log_message("Failed to fetch exchange rate. Exiting.")
        return

    log_message("Starting data collection session.")
    while True:
        data = fetch_data()
        if data:
            euro_Prices = []
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

            for value in data:
                if 'market' in value and 'price' in value:
                    if 'EUR' in value['market']:
                        euro_Prices.append({"timestamp": timestamp, "market": value['market'], "price": value['price']})
                    elif 'USDC' in value['market']:
                        converted_price = float(value['price']) * usd_to_eur_rate
                        euro_Prices.append({"timestamp": timestamp, "market": value['market'].replace('USDC', 'EUR'),
                                            "price": f"{converted_price:.8f}"})

            save_to_database(euro_Prices)
            log_message("[INFO] - Session done: Data saved to database.")
        else:
            log_message("Failed to fetch data in the current session.")

        time.sleep(10)

if __name__ == "__main__":
    main()
