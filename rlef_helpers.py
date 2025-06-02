import io
import json
import os
import pandas as pd
import requests
import ai_suggestions_promptexamples
import config
from crashanalytics import log_into_bigquery
from helper_func import project_exec_call
from models import storage_client
from logger import info_logger, error_logger
import traceback
from tenacity import retry, stop_after_attempt, wait_exponential
from analyser_repo_pipeline.alloydb_helpers import AlloyDBConnector

from repo_services_2 import extract_response_from_delimeters
from analysis_status_update import update_step_status
from utils.analysis_status_enums import StepStatus, PossibleStatuses

def save_mapping_to_alloydb(mapping, assistant_name, task_id):
    table_name = config.ORGANIZATION_NAME + '-' + assistant_name + "-filemap"

    try:
        alloydb_connector = AlloyDBConnector(
            database=config.EGPT_ALLOY_DB_NAME,
            username=config.EGPT_ALLOY_DB_USERNAME,
            password=config.EGPT_ALLOY_DB_PASSWORD,
            host=config.EGPT_ALLOY_DB_HOST,
            port=config.EGPT_ALLOY_DB_PORT
        )
        upsert_query_template = f"""
            INSERT INTO "{table_name}" (embed_id, rlef_resource_id, file_path, user_id, task_id, chunk_type)
            VALUES %s
            ON CONFLICT (embed_id) DO UPDATE SET
                rlef_resource_id = EXCLUDED.rlef_resource_id,
                file_path = EXCLUDED.file_path;
        """
        
        info_logger.info(f'Upserting data to the table, {table_name} for file_path and embedding id mapping')
        alloydb_connector.bulk_insert = True
        alloydb_connector.run(
            query=upsert_query_template,
            insertion_data=mapping,
            query_type="insert"
        )
        info_logger.info(f'Upsertion Completed in table {table_name}')
        return len(mapping)
    except Exception as e:
        error_logger.error(f"Error upserting data to the table {table_name} with error, {traceback.format_exc()}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILEMAP_INGESTION.value,
            status=PossibleStatuses.WARNING.value,
            error=f"Error upserting data to the table {table_name} with error, {traceback.format_exc()}",
            count=0,
            count_add_up=True
        )
        return 0

def update_deleted_file_on_alloydb(deleted_rowIds, assistant_name):
    try:
        table_name = f"{config.ORGANIZATION_NAME}-{assistant_name}-filemap"
        alloydb_connector = AlloyDBConnector(
            database=config.EGPT_ALLOY_DB_NAME,
            username=config.EGPT_ALLOY_DB_USERNAME,
            password=config.EGPT_ALLOY_DB_PASSWORD,
            host=config.EGPT_ALLOY_DB_HOST,
            port=config.EGPT_ALLOY_DB_PORT
        )

        # Handle single ID to ensure SQL syntax correctness
        if len(deleted_rowIds) == 1:
            id_tuple = f"('{deleted_rowIds[0]}')"
        else:
            id_tuple = tuple(deleted_rowIds)

        delete_query = f"""
            DELETE FROM "{table_name}"
            WHERE embed_id IN {id_tuple};
        """
        info_logger.info(f"Deleting rows with embed_ids {deleted_rowIds} from the table {table_name}")

        alloydb_connector.run(delete_query,query_type="delete")  # Using run from alloydb_helpers

        info_logger.info("Deletion Completed")
        return len(deleted_rowIds)

    except Exception as e:
        error_logger.error(f"Error deleting data from the table: {traceback.format_exc()}")
        return 0

def get_resource_from_rlef(user_id, task_id, collection_id, request_type="complete", chatbot_name=""):
    try:
        resource = []
        new_resource = []
        total_file_map_count = 0
        url = f"{config.RLEF_DOMAIN}/backend/collection/dataSet/{collection_id}/resourcesFile/downloadURL"
        # response = requests.get(requests.get(url).text)
        # df = pd.read_csv(io.StringIO(response.text), sep=",")
        response = requests.get(requests.get(url).text)
        try:
            df = pd.read_csv(io.StringIO(response.text), sep=",")
        except pd.errors.EmptyDataError as e:
            error_logger.error(f"Empty data error while parsing CSV: {str(e)}")
            return None, None, None, 0

        batch_data = []
        batch_id = ""
        delete_files = []
        embedId_resourceId_mapping = []
        save_batch_size = config.ALLOYDB_INSERT_BATCH_SIZE
        for i, (gcs_path, metadata) in enumerate(zip(df["GCStorage_file_path"], df['csv'])):
            file_path = download_gcp_file(df["GCStorage_file_path"][i], "")
            rlef_resourceId = df["_id"][i]
            embedId = json.loads(df["csv"][i]).get('pinecone_embeddingId', None)
            chunk_type = json.loads(df["csv"][i]).get('chunk', {}).get('chunk_type', 'file')

            if embedId is None:
                raise ValueError("RLEF embedId not found.")

            embedId_resourceId_mapping.append(
                (
                    embedId,
                    rlef_resourceId,
                    df['Prompt'][i],
                    user_id,
                    task_id,
                    chunk_type
                )
            )
            if len(embedId_resourceId_mapping) == save_batch_size:
                total_file_map_count += save_mapping_to_alloydb(embedId_resourceId_mapping, assistant_name=chatbot_name, task_id=task_id)
                embedId_resourceId_mapping = []
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    content = file.read()
            except FileNotFoundError as e:
                continue

            if content:
                try:
                    json_content = json.loads(content)
                    resource.append(json_content)

                    file_data = {
                        "file_id": json_content.get("file_id", ""),
                        "file_summary": json_content.get("summary", ""),
                        "update_type": json_content.get("update_type", ""),
                        "rlef_resourceId": rlef_resourceId,
                        "file_path": json_content.get("file", ""),
                        "git_url": json_content.get("git_url", ""),
                    }
                    batch_id = json_content.get("batch_id", "")
                    batch_data.append(file_data)

                    if file_data.get("update_type") == "deleted":
                        delete_files.append(embedId)

                except Exception as e:
                    resource.append(str(content))
            file.close()
            os.remove(file_path)

        if len(embedId_resourceId_mapping) > 0:
            total_file_map_count += save_mapping_to_alloydb(embedId_resourceId_mapping, assistant_name=chatbot_name, task_id=task_id)
            embedId_resourceId_mapping = []

        if delete_files:
            deleted_file_count = update_deleted_file_on_alloydb(delete_files,chatbot_name)

        for res in resource:
            summary_data = res.get("summary", "")
            service_data = res.get("chunk_details", {}).get('services', {}).get("external_services", [])
            extracted_services = '\n'.join([f"- {service_d.get('name', '')}" for service_d in service_data])
            extracted_summary, extraction_status = extract_response_from_delimeters(summary_data, "summary")
            
            res["summary"] = extracted_summary
            res["service_names"] = extracted_services

            new_resource.append(
                remove_keys_recursively(
                    dictionary=res,
                    keys_to_remove=[
                        "task_id",
                        "batch_id",
                        "content",
                        "file_id",
                    ],
                )
            )

        file_summaries = json.dumps(new_resource)

        # if request_type == "sync":
        #     batch_summary = ""
        # else:
        #     system_prompt,user_prompt = ai_suggestions_promptexamples.exe_summary_prompt(file_summaries)
        #     # batch_summary, status_code = project_exec_call(user_feedback=None, system_prompt=system_prompt, user_prompt=user_prompt, user_id=user_id)
        #     batch_summary = ""

        rlef_data = {
            "collection_id": collection_id,
            "batch_id": batch_id,
            "batch_summary": "",
            "batch_data": batch_data,
        }

        
        return "", file_summaries, rlef_data, total_file_map_count
    except Exception as e:
        error_logger.error(f"Error in get_resource_from_rlef: {str(e)}")
        traceback.print_exc()
        log_into_bigquery("get_resource_from_rlef", user_id, task_id, str(e), 400)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILEMAP_INGESTION,
            status=PossibleStatuses.WARNING.value,
            error=f"Error in get_resource_from_rlef: {traceback.format_exc()}",
            count=0,
            count_add_up=True
        )
        return None, None, None, 0


def download_gcp_file(URL, folder_path):
    file_path = os.path.join(folder_path, os.path.basename(URL))
    if "gs://" in URL:
        gcs = URL.replace("gs://", "")
        bucket = gcs.split("/")[0]
        # print("Bucket: ", bucket)
        URL = gcs.replace(f"{bucket}/", "")
        # print("URL: ", URL)
        bucket = storage_client.bucket(bucket)
    else:
        return False
    blob = bucket.blob(URL)
    # print("Blob: ",blob)
    blob.download_to_filename(file_path)
    return file_path


def remove_keys_recursively(dictionary, keys_to_remove):
    """
    Recursively removes specified keys from a dictionary.

    :param dictionary: The dictionary to process.
    :param keys_to_remove: The keys to remove from the dictionary.
    :return: The dictionary with the specified keys removed.
    """
    if not isinstance(dictionary, dict):
        return dictionary

    # Create a new dictionary to avoid modifying the original dictionary in place
    new_dict = {}

    for key, value in dictionary.items():
        if key in keys_to_remove:
            continue

        if isinstance(value, dict):
            # Recursively process nested dictionaries
            new_dict[key] = remove_keys_recursively(value, keys_to_remove)
        elif isinstance(value, list):
            # If the value is a list, process each item in the list
            new_dict[key] = [
                remove_keys_recursively(item, keys_to_remove)
                if isinstance(item, dict)
                else item
                for item in value
            ]
        else:
            new_dict[key] = value

    return new_dict


def upload_txt_autoai(
    file_name,
    file_content,
    model_id,
    labels,
    tag,
    csv="None",
    prompt=None,
    confidence_score=100,
):
    try:
        rlef_domain = config.RLEF_DOMAIN
        url = f"{rlef_domain}/backend/resource/"

        payload = {
            "model": model_id,
            "status": "backlog",
            "csv": csv,
            "label": labels,
            "tag": tag,
            "prediction": "predicted",
            "confidence_score": confidence_score,
        }

        if prompt is not None:
            payload["prompt"] = prompt
        else:
            payload["prompt"] = file_name

        files = [("resource", (file_name, file_content))]

        headers = {}

        response = requests.request(
            "POST", url, headers=headers, data=payload, files=files
        )

        # print(response.text)
        info_logger.info(f"Response from upload_txt_autoai: {response.text} with status code: {response.status_code}")
        return response.text

    except Exception as e:
        print("Error in upload_txt_autoai: ", str(e))
        return None
    


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=1, max=5))
def upload_dependency_rlef_get_api(
    file_name,
    file_content,
    model_id,
    labels,
    tag,
    csv="None",
    prompt=None,
    confidence_score=100,
):
    try:
        rlef_domain = config.RLEF_DOMAIN
        url = f"{rlef_domain}/backend/resource/"

        payload = {
            "model": model_id,
            "status": "backlog",
            "csv": csv,
            "label": labels,
            "tag": tag,
            "prediction": "predicted",
            "confidence_score": confidence_score,
            "resourceFileName": file_name,
            "appShouldNotUploadResourceFileToGCS": "true",
            "resourceContentType": "text/plain",
        }

        files = []
        headers = {}

        response = requests.post(url, headers=headers, data=payload, files=files)

        info_logger.info(f"Response from upload_txt_autoai: {response.text} with status code: {response.status_code}")

        if response.status_code == 200:
            gcp_path = response.json().get("resourceFileSignedUrlForUpload")
            info_logger.info(f"Resource File Signed URL: {gcp_path}")
            upload_txt_autoai_to_gcp(file_name, file_content, gcp_path)
            return response.text
        else:
            error_logger.error(f"Failed to upload dependency. Status Code: {response.status_code}")
            return response.text

    except Exception as e:
        error_logger.error(f"Error in upload_dependency_rlef_get_api: {str(e)} Traceback: {traceback.format_exc()}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=1, max=5))
def upload_txt_autoai_to_gcp(file_name, file_content_or_path, gcs_url):
    """
    Upload text content to Google Cloud Storage using a signed URL.
    
    Args:
        file_name (str): Name of the file
        file_content_or_path (str or bytes): Either the content of the file as string/bytes,
                                           or a path to the file
        gcs_url (str): Google Cloud Storage signed URL to upload to
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        if isinstance(file_content_or_path, str) and os.path.exists(file_content_or_path):
            with open(file_content_or_path, "rb") as file:
                data = file.read()
        else:
            data = file_content_or_path
            if isinstance(data, str):
                data = data.encode("utf-8")

        headers = {"Content-Type": "text/plain"}

        response = requests.put(url=gcs_url, headers=headers, data=data)

        if response.status_code in [200, 201, 204]:
            info_logger.info(f"Dependency File {file_name} successfully uploaded to GCS")
            return True
        else:
            error_logger.error(f"Failed to upload dependency file. Status code: {response.status_code}")
            error_logger.error(f"Response: {response.text}")
            return False

    except Exception as e:
        error_logger.error(f"Error uploading file to GCS: {str(e)} Traceback: {traceback.format_exc()}")
        raise

def get_data_from_rlef(rlef_resource_id):
    url = f"{config.RLEF_DOMAIN}/backend/resource/{rlef_resource_id}"
    DEFAULT_TIMEOUT = 30  # seconds

    if not rlef_resource_id:
        error_logger.error("rlef_resource_id cannot be None or empty.")
        return None

    payload = {}
    headers = {
    'accept': 'application/json'
    }

    info_logger.info(f"Attempting to fetch data for rlef_resource_id: {rlef_resource_id}")
    info_logger.info(f"Requesting URL: {url} with headers: {headers}")

    response = requests.request("GET", url, headers=headers, data=payload).json()    
    
    try:
        response = requests.get(url, headers=headers, json=payload if payload else None, timeout=DEFAULT_TIMEOUT)

        # Check for HTTP errors (4xx or 5xx status codes)
        response.raise_for_status()

        info_logger.info(f"Successfully fetched data for {rlef_resource_id}. Status code: {response.status_code}")

        try:
            response_data = response.json()
            info_logger.info(f"Successfully parsed JSON response for {rlef_resource_id}.")
            return response_data
        except requests.exceptions.JSONDecodeError as json_err: # More specific to requests
            error_logger.error(f"Failed to decode JSON response from {url}. Error: {json_err}")
            return None

    except requests.exceptions.HTTPError as http_err:
        error_logger.error(f"HTTP error occurred while fetching {url}: {http_err}")
        info_logger.info(f"Response content: {response.text[:500] if response and response.text else 'No response text'}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        error_logger.error(f"Connection error occurred while fetching {url}: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        error_logger.error(f"Request to {url} timed out: {timeout_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        error_logger.error(f"An unexpected error occurred with the request to {url}: {req_err}")
        return None
    except Exception as e:
        error_logger.error(f"An unexpected non-request error occurred for {rlef_resource_id} at {url}: {e}", exc_info=True)
        return None