import pandas as pd
import numpy as np
from datetime import datetime
import random

# Parameters
regions = ['Punjab', 'Haryana', 'Rajasthan']
crop_types = ['Wheat', 'Maize', 'Rice']
soil_types = ['Clay', 'Loamy', 'Sandy']
n_samples = 100

# Generate synthetic data
data = {
    "Region": np.random.choice(regions, n_samples),
    "Crop Type": np.random.choice(crop_types, n_samples),
    "Rainfall (mm)": np.random.uniform(200, 800, n_samples).round(1),
    "Temperature (°C)": np.random.uniform(15, 35, n_samples).round(1),
    "Soil pH": np.random.uniform(5.5, 7.5, n_samples).round(1),
    "Nitrogen (ppm)": np.random.uniform(20, 60, n_samples).round(1),
    "Sowing Date": [datetime(2023, random.randint(1, 6), random.randint(1, 28)).strftime("%Y-%m-%d") for _ in range(n_samples)],
    "Harvest Date": [datetime(2023, random.randint(7, 12), random.randint(1, 28)).strftime("%Y-%m-%d") for _ in range(n_samples)],
    "Yield (tons/ha)": np.random.uniform(1.5, 5.5, n_samples).round(1),
    "Soil Type": np.random.choice(soil_types, n_samples)
}

# Create DataFrame
df_synthetic_crop_yield = pd.DataFrame(data)

# Save to CSV
df_synthetic_crop_yield.to_csv("synthetic_crop_yield_dataset.csv", index=False)

