from collections import Counter
from multiprocessing import Pool
import csv

# Define the map function
def map_passenger_flights(record):
    passenger_id, _ = record
    return (passenger_id, 1)

# Define the reduce function
def reduce_passenger_flights(counts1, counts2):
    return counts1 + counts2

# Load and preprocess passenger flights data
def preprocess_passenger_flights_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header if present
        for row in csv_reader:
            passenger_id = row[0]
            data.append((passenger_id, 1))
    return data

# Main function to execute MapReduce
def main():
    # Load and preprocess passenger flights data
    passenger_flights_data = preprocess_passenger_flights_data("AComp_Passenger_data_no_error.csv")

    # Map phase
    with Pool() as pool:
        mapped_data = pool.map(map_passenger_flights, passenger_flights_data)

    # Reduce phase
    counts = Counter()
    for passenger_id, count in mapped_data:
        counts[passenger_id] += count

    # Find passenger(s) with the highest number of flights
    max_flight_count = max(counts.values())
    passengers_with_max_flights = [passenger_id for passenger_id, count in counts.items() if count == max_flight_count]

    # Print the result
    print("Passenger(s) with the highest number of flights:")
    for passenger_id in passengers_with_max_flights:
        print(f"Passenger ID: {passenger_id}, Number of Flights: {max_flight_count}")

if __name__ == "__main__":
    main()
