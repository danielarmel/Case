import dlt
import requests
from datetime import datetime
# test
# 1. Extract dog breed data from TheDogAPI
def dog_breeds():
    response = requests.get("https://api.thedogapi.com/v1/breeds")
    response.raise_for_status()
    breeds = response.json()
    timestamp = datetime.utcnow().isoformat()

    for breed in breeds:
        yield {
            "timestamp": timestamp,
            "id": breed.get("id"),
            "name": breed.get("name"),
            "bred_for": breed.get("bred_for"),
            "breed_group": breed.get("breed_group"),
            "life_span": breed.get("life_span"),
            "temperament": breed.get("temperament"),
            "origin": breed.get("origin"),
            "reference_image_id": breed.get("reference_image_id"),
            "weight_imperial": breed.get("weight", {}).get("imperial"),
            "weight_metric": breed.get("weight", {}).get("metric"),
            "height_imperial": breed.get("height", {}).get("imperial"),
            "height_metric": breed.get("height", {}).get("metric"),
        }

# 2. Set up DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="dog_breeds_pipeline",
    destination="bigquery",
    dataset_name="bronze"
)

# 3. Run the pipeline
load_info = pipeline.run(
    dog_breeds(),
    table_name="dog_api_raw",
    write_disposition="append"
)
#test
print(f"✅ Loaded to BigQuery: {load_info}")
# test