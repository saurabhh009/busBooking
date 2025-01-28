import pandas as pd
import random
from datetime import datetime, timedelta

def generate_booking_data(num_records):
    bus_ids = [f'Bus_{i}' for i in range(1, 21)]  # 20 buses
    routes = [f'Route_{i}' for i in range(1, 11)]  # 10 routes
    data = []

    for _ in range(num_records):
        booking_time = datetime.now() - timedelta(days=random.randint(0, 30))
        bus_id = random.choice(bus_ids)
        route = random.choice(routes)
        passengers_count = random.randint(1, 50)  # Random passenger count
        data.append((booking_time, bus_id, route, passengers_count))

    df = pd.DataFrame(data, columns=['booking_time', 'bus_id', 'route', 'passengers_count'])
    df.to_csv('bus_bookings.csv', index=False)

generate_booking_data(1000)  # Generate 1000 records
