# Load the uploaded FAO dataset to inspect the data structure

import pandas as pd



# Load dataset

file_path = "/mnt/data/FAOSTAT_data_en_11-6-2024.csv"

fao_data = pd.read_csv(file_path)




fao_data.head(), fao_data.columns
