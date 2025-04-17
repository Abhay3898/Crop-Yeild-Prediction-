from flask import Flask, render_template, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.linalg import Vectors
from datetime import datetime

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("CropYieldPrediction").getOrCreate()

# Load trained Random Forest model
model_path = "models/random_forest_model"  # Ensure this path is correct
model = RandomForestRegressionModel.load(model_path)

# StringIndexer mappings (hardcoded based on training data index)
# Update these mappings to match your synthetic dataset
region_map = {
    "Nagpur": 0.0, "Amravati": 1.0, "Latur": 2.0, "Pune": 3.0, "Aurangabad": 4.0,
    "Nashik": 5.0, "Solapur": 6.0, "Ratnagiri": 7.0, "Kolhapur": 8.0, "Satara": 9.0
}

crop_map = {
    "Wheat": 0.0, "Jowar": 1.0, "Bajra": 2.0, "Maize": 3.0, "Toor": 4.0, "Moong": 5.0, "Rice": 6.0
}

soil_map = {
    "Loamy": 0.0, "Clay": 1.0, "Sandy": 2.0, "Laterite": 3.0
}

@app.route('/')
def home():
    return render_template('indexmain.html')  # Make sure your frontend is named index.html

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Fetch input from form
        region = request.form.get('region')
        crop = request.form.get('crop')
        soil = request.form.get('soil')
        rainfall = float(request.form.get('rainfall'))
        temperature = float(request.form.get('temperature'))
        ph = float(request.form.get('ph'))
        nitrogen = float(request.form.get('nitrogen'))
        sowing = request.form.get('sowing')
        harvest = request.form.get('harvest')

        # Convert dates to duration in days
        sowing_date = datetime.strptime(sowing, '%Y-%m-%d')
        harvest_date = datetime.strptime(harvest, '%Y-%m-%d')
        duration = (harvest_date - sowing_date).days

        # Convert categorical to index
        region_idx = region_map.get(region, 0.0)
        crop_idx = crop_map.get(crop, 0.0)
        soil_idx = soil_map.get(soil, 0.0)

        # Feature order must match training: [region, crop, rainfall, temp, ph, nitrogen, duration, soil]
        features = [
            region_idx, crop_idx, rainfall, temperature,
            ph, nitrogen,soil_idx
        ]

        # Spark DataFrame with features
        df = spark.createDataFrame([(Vectors.dense(features),)], ["features"])

        # Predict yield
        prediction = model.transform(df).select("prediction").collect()[0][0]

        return render_template('indexmain.html', prediction=round(prediction, 2))

    except Exception as e:
        return render_template('indexmain.html', error=str(e))

if __name__ == '__main__':
    app.run(debug=True)
