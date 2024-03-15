from collections import Counter
from multiprocessing import Pool
import csv
import sys
import os.path  

# Define the map function
def map_passenger_flights(record):
    passenger_id, _ = record
    return (passenger_id, 1)

# Define the reduce function
def reduce_passenger_flights(counts1, counts2):
    return counts1 + counts2

# Define the shuffle function
def shuffle(mapped_data):
    counts = Counter()
    for passenger_id, count in mapped_data:
        counts[passenger_id] += count
    return counts.items()

# Load and preprocess passenger flights data
def preprocess_passenger_flights_data(file_path):
    data = []
    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                if len(row) >= 1:  # Check if the row has at least one element
                    passenger_id = row[0]
                    data.append((passenger_id, 1))
                else:
                    print("Error: Invalid row in CSV file.")
    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print(f"Error: An unexpected error occurred - {e}")
    return data

# Main function to execute MapReduce
def main():
    try:
        # Prompt the user to enter the file path
        file_path = input("Enter the file path for passenger flights data: ")

        # Validate the file path
        if not file_path.strip():
            print("Error: File path cannot be empty.")
            sys.exit(1)
        if not os.path.exists(file_path):
            print("Error: File not found.")
            sys.exit(1)
        if not os.path.isfile(file_path):
            print("Error: Invalid file path. Please provide a valid file path.")
            sys.exit(1)

        # Load and preprocess passenger flights data
        passenger_flights_data = preprocess_passenger_flights_data(file_path)

        if not passenger_flights_data:
            print("Error: No data found in the file.")
            sys.exit(1)

        # Map phase
        with Pool() as pool:
            mapped_data = pool.map(map_passenger_flights, passenger_flights_data)

        # Shuffle phase
        shuffled_data = shuffle(mapped_data)

        # Reduce phase
        counts = Counter()
        for passenger_id, count in shuffled_data:
            counts[passenger_id] += count

        # Find passenger(s) with the highest number of flights
        max_flight_count = max(counts.values())
        passengers_with_max_flights = [passenger_id for passenger_id, count in counts.items() if count == max_flight_count]

        # Print the result
        print("Passenger(s) with the highest number of flights:")
        for passenger_id in passengers_with_max_flights:
            print(f"Passenger ID: {passenger_id}, Number of Flights: {max_flight_count}")

    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
        sys.exit(1)

if __name__ == "__main__":
    main()