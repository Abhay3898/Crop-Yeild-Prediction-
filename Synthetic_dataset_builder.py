import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Define regions and crop-specific soil types
regions = ['Nagpur', 'Amravati', 'Latur', 'Pune', 'Aurangabad', 'Nashik', 'Solapur', 'Ratnagiri', 'Kolhapur', 'Satara']
crops = ['Wheat', 'Jowar', 'Bajra', 'Maize', 'Toor', 'Moong', 'Rice']
soil_mapping = {
    'Wheat': ['Loamy', 'Clay'],
    'Jowar': ['Sandy', 'Loamy'],
    'Bajra': ['Sandy', 'Loamy'],
    'Maize': ['Loamy', 'Sandy'],
    'Toor': ['Loamy', 'Clay'],
    'Moong': ['Sandy', 'Loamy'],
    'Rice': ['Clay', 'Laterite']
}

data = []

for _ in range(1000):
    region = random.choice(regions)
    crop = random.choice(crops)
    soil = random.choice(soil_mapping[crop])
    rainfall = round(np.random.uniform(600, 1400), 1) if crop == 'Rice' else round(np.random.uniform(400, 1100), 1)
    temperature = round(np.random.uniform(20, 35), 1)
    ph = round(np.random.uniform(5.5, 7.5), 1)
    nitrogen = round(np.random.uniform(30, 70), 1)

    sowing_date = datetime.strptime(f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}", "%Y-%m-%d")
    duration = random.randint(90, 160)
    harvest_date = sowing_date + timedelta(days=duration)

    yield_tons_per_ha = round(np.random.uniform(2.0, 5.5), 2)

    data.append([
        region, crop, rainfall, temperature, ph, nitrogen,
        sowing_date.strftime('%Y-%m-%d'),
        harvest_date.strftime('%Y-%m-%d'),
        yield_tons_per_ha, soil
    ])

# DataFrame
columns = [
    'Region', 'Crop Type', 'Rainfall (mm)', 'Temperature (°C)', 'Soil pH', 'Nitrogen (ppm)',
    'Sowing Date', 'Harvest Date', 'Yield (tons/ha)', 'Soil Type'
]
df = pd.DataFrame(data, columns=columns)
df.to_csv("Synthetic_Crops_dataset.csv", index=False)
print("✅ Dataset generated: ")

