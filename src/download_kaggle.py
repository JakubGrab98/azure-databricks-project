import os
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


load_dotenv()


KAGGLE_DATASET = "artyomkruglov/gaming-profiles-2025-steam-playstation-xbox"
LOCAL_EXTRACT_PATH="./datasets"
AZURE_STORAGE_ACCOUNT_NAME = "dbrgamingstorage"
AZURE_CONTAINER_NAME = "raw"
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
DATA_FOLDERS = ["playstation", "steam", "xbox"]

# api = KaggleApi()
# api.authenticate()

# api.dataset_download_files(KAGGLE_DATASET, path=LOCAL_EXTRACT_PATH, unzip=True)

blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)

for folder_name in DATA_FOLDERS:
    folder_path = os.path.join("datasets", folder_name)

    if os.path.exists(folder_path):
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=f"{folder_name}/{file_name}")

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
