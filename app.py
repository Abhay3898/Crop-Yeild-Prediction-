from flask import Flask, render_template, request, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.linalg import Vectors

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("CropYieldPrediction").getOrCreate()

# Load the trained model (Make sure to save & use your trained model)
model_path = "models/random_forest_model"  # Update this path
model = RandomForestRegressionModel.load(model_path)

@app.route('/')
def home():
    return render_template('index.html')  # Serve the frontend

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get user input from form
        data = request.json
        features = [float(x) for x in data.values()]  # Convert to float
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame([(Vectors.dense(features),)], ["features"])
        
        # Make prediction
        prediction = model.transform(df).select("prediction").collect()[0][0]

        return jsonify({'prediction': round(prediction, 2)})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
