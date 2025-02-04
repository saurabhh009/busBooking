import pandas as pd
import random

# Define 20 routes and months
routes = [f"Route {i}" for i in range(1, 21)]
years = [2021, 2022, 2023]
months = list(range(1, 13))  # 1 to 12 (January to December)

data = []

for _ in range(1000):
    year = random.choice(years)
    month = random.choice(months)
    route = random.choice(routes)
    num_trips = random.randint(100, 1000)
    passengers = num_trips * random.randint(20, 50)
    data.append([year, month, route, num_trips, passengers])

# Convert to DataFrame and save as CSV
df = pd.DataFrame(data, columns=["Year", "Month", "Route", "Num_Trips", "Passengers"])
csv_filename = "bus_travel_data.csv"
df.to_csv(csv_filename, index=False)

print(f"CSV file '{csv_filename}' generated with {len(df)} records.")
