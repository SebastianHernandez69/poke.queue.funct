import azure.functions as func
import json
import logging
import requests
from dotenv import load_dotenv
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io

app = func.FunctionApp()

load_dotenv()

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_CONTAINER_NAME = os.getenv("STORAGE_CONTAINER_NAME")

@app.queue_trigger(
        arg_name="azqueue", 
        queue_name="requests",
        connection="sapokequeuedevseb_STORAGE") 

def QueueTriggerPokeRequest(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request(id, "inprogress")

    request = get_request(id)
    pokemons = get_pokemons(request["type"])

    pokemons_bytes = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    
    upload_csv_to_blob( blob_name=blob_name, csv_data=pokemons_bytes )
    logging.info(f"{blob_name} file uploaded successfully")

    full_url = f"https://{STORAGE_CONTAINER_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request(id, "completed", full_url)


def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status,
        "id": id
    }

    if url:
        payload["url"] = url

    response = requests.put(f"{ DOMAIN }/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{ DOMAIN }/api/request/{id}")
    return response.json()[0]


def get_pokemons(type: str) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"

    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    poke_entries = data.get("pokemon", [])

    return [p["pokemon"] for p in poke_entries]


def generate_csv_to_blob(pokemons_list: list) -> bytes:
    df = pd.DataFrame( pokemons_list) 

    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()

    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client( container=BLOB_CONTAINER_NAME, blob=blob_name )
        blob_client.upload_blob( csv_data, overwrite = True )

    except Exception as e:
        logging.error(f"Error uploading CSV to blob: {e}")
        raise