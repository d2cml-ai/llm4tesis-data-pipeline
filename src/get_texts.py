from typing import Any
from Constants import TEMP_DIR_PATH, TESIS_COLLECTIONS, PUCP_REST_ADDRESS
from Secrets import RAW_DATA_CONTAINER, RAW_DATA_BLOB_NAME
import requests
from collections import Counter
import os
from cleanup import cleanup
import json
import shutil
from azure.storage.blob import BlobClient
from blob_operations import get_blob_client, get_blob_service

def metadata_to_dict(metadata_keys: list[str], metadata_values: list[str]) -> dict[str, list[str]]:
        metadata: dict[str, list[str]] = {}
        for key, value in zip(metadata_keys, metadata_values):
                if key not in metadata.keys():
                        metadata.update({key: [value]})
                else:
                        metadata[key].append(value)
        return metadata

def get_entries_metadata(item_uuid: str) -> dict[str, list[str]]:
        item_metadata_request: requests.Response = requests.get(
                f"{PUCP_REST_ADDRESS}items/{item_uuid}/metadata"
        )
        entry_metadata: list[dict[str, Any]] =  item_metadata_request.json()
        metadata_keys: list[str] = [entry["key"] for entry in entry_metadata]
        metadata_values: list[str] = [entry["value"] for entry in entry_metadata]
        item_metadata: dict[str, list[str]] = metadata_to_dict(metadata_keys, metadata_values)
        return item_metadata

def write_plaintext(path: str, retrieval_link: str, uuid: str) -> None:
        plaintext_request: requests.Response = requests.get(
                f"https://tesis.pucp.edu.pe{retrieval_link}"
        )
        plaintext_write_path: str = os.path.join(path, f"{uuid}.txt")
        with open(plaintext_write_path, "w", encoding="latin1") as f:
                f.write(plaintext_request.text)

def get_open_document(path: str, uuid: str, item_metadata: dict[str, list[str]]) -> None:
        bitstream_request: requests.Response = requests.get(
                f"{PUCP_REST_ADDRESS}items/{uuid}/bitstreams"
        )
        item_bitstreams: list[dict[str, Any]] = bitstream_request.json()
        bitstream_mimetypes: list[str] = [bitstream["mimeType"] for bitstream in item_bitstreams]
        for index, mimetype in enumerate(bitstream_mimetypes):
                if mimetype == "text/plain":
                        retrieval_link: str = item_bitstreams[index]["retrieveLink"]
                        write_plaintext(path, retrieval_link, uuid)
                        item_metadata["plaintext_available"] = ["Yes"]

def main() -> None:
        
        metadata_dir_path: str = os.path.join(TEMP_DIR_PATH, RAW_DATA_BLOB_NAME)
        os.makedirs(metadata_dir_path)
        all_metadata: dict[str, dict[str, list[str]]] = {}
        
        for key, value in TESIS_COLLECTIONS.items():

                print(f"Getting list of items in collection '{key}'")
                items_request: requests.Response = requests.get(
                        f"{PUCP_REST_ADDRESS}collections/{value}/items?limit=1000"
                )
                items: list[dict[str, Any]] = items_request.json()

                print(f"Retrieving metadata and downloading text for all {len(items)} items")
                print(" ---+--- 1 ---+--- 2 ---+--- 3 ---+--- 4 ---+--- 5")
                for index, item in enumerate(items):
                        print(".", end = "")
                        if (index + 1) % 50 == 0: print(f" {index + 1}")
                        item_uuid: str = item["uuid"]
                        item_metadata: dict[str, list[str]] = get_entries_metadata(item_uuid)
                        if "info:eu-repo/semantics/openAccess" in item_metadata["dc.rights"]:
                                get_open_document(metadata_dir_path, item_uuid, item_metadata)
                        if "plaintext_available" not in item_metadata.keys():
                                item_metadata["plaintext_available"] = ["No"]
                        all_metadata.update({item_uuid: item_metadata})
        
        print("Writing metadata JSON")        
        metadata_file_path: str = os.path.join(metadata_dir_path, "metadata.json")

        with open(metadata_file_path, "w", encoding="latin1") as f:
                json.dump(all_metadata, f)

        print("Zipping and uploading blob")
        shutil.make_archive(metadata_dir_path, "zip", metadata_dir_path)
        blob_client: BlobClient = get_blob_client(RAW_DATA_BLOB_NAME + ".zip", RAW_DATA_CONTAINER, get_blob_service())
        with open(metadata_dir_path + ".zip", "rb") as blob:
                blob_client.upload_blob(blob, overwrite = True)


if __name__ == "__main__":
        try:
                main()
        except Exception as e:
                raise e
        finally:
                cleanup()




