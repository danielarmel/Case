import dlt
import requests

@dlt.resource(name="dog_breeds")
def dog_breeds():
    response = requests.get("https://dog.ceo/api/breeds/list/all")
    response.raise_for_status()

    breeds = response.json()["message"]
    for breed, subbreeds in breeds.items():
        yield {
            "breed": breed,
            "subbreeds": subbreeds
        }

pipeline = dlt.pipeline(
    pipeline_name="dog_pipeline",
    destination="bigquery",
    dataset_name="bronze"  # BigQuery dataset name
)

load_info = pipeline.run(dog_breeds())
print(load_info)
