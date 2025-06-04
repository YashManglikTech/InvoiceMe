import os
import traceback
import uuid
from flask import jsonify
import json
from typing import Dict, List, Tuple

from absolute_path.language_support import get_supported_languages_dict, read_json_from_bucket
from analyser_repo_pipeline.api_helpers import _handle_error
from analyser_repo_pipeline.crypto_helpers import decrypting_token, encrypting_token
from analyser_repo_pipeline.pipeline_helpers import ThreadWithReturnValue, fetch_repository_summaries, handle_mongo_upload
from analyser_repo_pipeline.rlef_helpers import get_resource_from_rlef
from clone_repo import check_repository_changes
from db_update import update_batch_summaries_in_db, update_database_with_rlef_ids_and_chatbot_name
from generalised_utils import fetch_user_email
from helper_func import check_analysed_repo
from ingest_project_summary import repo_already_analyzed_v2, update_status_project_analyzer_database
from models import project_summary_db
from logger import info_logger, error_logger, warning_logger
from sync_project_summary.ai_helpers import process_batch_data
from sync_project_summary.db_helpers import get_intial_analysis_data
from datetime import datetime, timezone
import config
from sync_project_summary.egpt_helpers import sync_github_egpt_call
from analysis_status_update import create_task_status, update_step_status
from utils.analysis_status_enums import StepStatus, PossibleStatuses


def check_if_resync_supported(task_id, user_id):
    try:
        initial_data = get_intial_analysis_data(task_id, user_id)
        # info_logger.info(f"Initial data for task_id {task_id}: {initial_data}")
        if not initial_data:
            return False, f"Task ID {task_id} not found for user ID {user_id}", {}, 404
        
        repo_status, created_time, task_id  = repo_already_analyzed_v2(
            initial_data["repo_urls"],
            initial_data["branch_names"],
            user_id,
            task_id,
            initial_data["project_name"],
        )
        # info_logger.info(f"Repo status for task_id {task_id}: {repo_status}")

        if repo_status == "Analysis In Progress":
            return False, "Analysis In Progress", initial_data, 409


        elif repo_status == "Repository Already Analyzed":

            info_logger.info("Checking if there any changes are made to the repository")
            is_changed , results, commit_ids = check_commit_changes(initial_data["git_info"])
            if not is_changed:
                warning_logger.warning("No changes detected in the repository")
                warning_logger.warning(f"Results: {results}")
                return False, "No Changes Detected in the Repository", initial_data, 304
            
            
            for i, repo in enumerate(initial_data["git_info"]):
                if i < len(commit_ids):
                    repo['previous_commit_hash']= repo["commit_hash"]
                    repo["commit_hash"] = commit_ids[i]
                    repo['changed_files']=[i.get('changed_files',[]) for i in results]
            
            info_logger.info(f"Changes detected in the repository: {results}")
            current_version, old_project_id, status = check_analysed_repo(user_id=user_id, github_urls=initial_data["git_info"], branch_names=initial_data["branch_names"])
            version = current_version + 1 if current_version is not None else 1
            initial_data["version"] = version
            
            approx_output_cost = (1000 / 1_000_000) * 15
            initial_data["approx_output_cost"] = approx_output_cost
            
            is_gcp_supported = initial_data.get("is_gcp_support", False)
            if is_gcp_supported:
                rlef_data_gcp_path = initial_data.get("dashboard", {}).get("rlef_data", None)
                if rlef_data_gcp_path:
                    rlef_data = read_json_from_bucket(config.GCP_BUCKET_BASE_PATH, rlef_data_gcp_path)
                    initial_data["dashboard"]["rlef_data"] = rlef_data
                else:
                    initial_data["dashboard"]["rlef_data"] = []


            info_logger.info(f"Github Info for task_id {task_id}: {initial_data['git_info']}")
            return True, "Repository Already Analyzed", initial_data, 200
        
    except Exception as e:
        print(f"Error in check_if_resync_supported: {e} Traceback: {traceback.format_exc()}")
        return False, "Error", {}, 500
    
    
    
def _initialise_resync_project(resync_task_id,user_id, old_task_id, sync_type, initial_data):
    """
    Initialise resync project
    """
    try:
        update_status_project_analyzer_database(
            task_id=resync_task_id,
            user_id=user_id,
            batch_size=initial_data["batch_size"],
            project_name=initial_data["project_name"],
            data=None,
            analysis_type="sync",
            completion_status=False
        )

        create_task_status(
            task_id=resync_task_id,
            user_id=user_id,
            analysis_type="sync"
        )

        project_data = {
            "parent_id": old_task_id,
            "task_id": resync_task_id,
            "user_id": user_id,
            "repo_urls": initial_data["repo_urls"],
            "git_info" : initial_data["git_info"],
            "branch_names": initial_data["branch_names"],
            "attempts": {
                "executive_summary": 0,
                "feature_hierarchy": 0,
                "vulnerability_check": 0,
                "services_used": 0,
                "feature_summary": 0
            },
            "uploaded_file": 0,
            "description": initial_data["description"],
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "id": str(uuid.uuid4()),
            "name": initial_data["assistant_id"],
            "version": initial_data["version"],
            "batch_size": 25,
            "project_name": initial_data["project_name"],
            "assistant_id": initial_data["assistant_id"],
            "project_id": initial_data["project_id"],
            "is_gcp_support": True,
        }
        

        result = project_summary_db.projects.insert_one(project_data)
        if result.inserted_id:
            info_logger.info(f"Resync project created with task_id: {resync_task_id}")


        return resync_task_id
    except Exception as e:
        print(f"Error in _initialise_resync_project: {e}")
        return None


def sync_repositories_egpt(task_id, github_urls, git_pat_token, branch_names, assistant_id):
    upload_threads = []
    for github_url, branch_name in zip(github_urls, branch_names):
        complete_repo_url = f"https://{git_pat_token}@{github_url.split('https://')[1]}"
        upload_thread = ThreadWithReturnValue(target=sync_github_egpt_call, args=(task_id, complete_repo_url, branch_name, assistant_id))
        upload_threads.append(upload_thread)
        upload_thread.start()

    return upload_threads



def extract_rlef_collection_ids(sync_results: List[tuple] = None, sync_type: str = "soft", task_id: str = "", user_id: str = "", project_name: str =  "", chatbot_name:str = "", github_urls: list = [], old_rlef_ids:str = "" ) -> List[str]:
    """
    Extract rlef_collectionId values from successful sync results (status code 200)

    Args:
        sync_results: List of tuples containing (json_string, status_code)

    Returns:
        List of rlef_collectionId values
    """
    collection_ids = []
    try:
        if sync_type == "soft":

            for result, status_code in sync_results:
                if status_code == 200:
                    try:
                        data = json.loads(result)

                        files = data.get('data', [{}])[0].get('data', {}).get('files', [])

                        for file in files:
                            if 'rlef_collectionId' in file:
                                collection_ids.append(file['rlef_collectionId'])

                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        print(f"Error processing result: {e}")
                        continue
                    
            
            final_rlef_ids = old_rlef_ids + collection_ids
            update_database_with_rlef_ids_and_chatbot_name(task_id, final_rlef_ids, project_name)

        else:
            collection_ids = handle_mongo_upload(user_id, task_id, chatbot_name, project_name, github_urls)



        return collection_ids
    except Exception as e:
        print(f"Error in extract_rlef_collection_ids: {e}")
        return []





def sync_project_summary(user_id, task_id):
    """
    Sync project summary
    """
    try:
        resync_supported, status, initial_data = check_if_resync_supported(task_id, user_id)
        if resync_supported:
            resync_task_id = _initialise_resync_project(user_id, task_id, status, initial_data)
            if resync_task_id:
                sync_threads = sync_repositories_egpt(task_id=task_id, github_urls=initial_data["repo_urls"], branch_names=initial_data["branch_names"], git_pat_token=initial_data["git_info"], assistant_id=initial_data["assistant_id"])
                sync_thread_results = [thread.join() for thread in sync_threads]
                collection_ids = extract_rlef_collection_ids(sync_thread_results)
                info_logger.info(f"Collection IDs: {collection_ids}")

                return jsonify({"status": "Resync Started", "task_id": resync_task_id})
            else:
                return jsonify({"status": "Error", "message": "Resync task creation failed"})
        else:
            return jsonify({"status": status, "message": "Resync not supported"})
    except Exception as e:
        print(f"Error in sync_project_summary: {e}")
        return jsonify({"status": "Error", "message": str(e)})


def sync_rlef_file_summaries(collections_ids, user_id, task_id, initial_data, max_threads=10, sync_type="soft", chatbot_name=""):
    updated_batch_summary_list = []
    updated_file_summary_list = []
    updated_rlef_data_list = []
    info_logger.info(f"Fetching summaries for sync_type: {sync_type}")

    if sync_type == "soft":
        batch_summary_list = []
        file_summary_list = []
        rlef_data_list = []
        collection_threads = []
        processed_collections = set()

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILEMAP_INGESTION.value,
            status=PossibleStatuses.IN_PROGRESS.value,
            count=0
        )
        total_count = 0
        try:
            for collection_id in collections_ids:
                if collection_id is not None and collection_id not in processed_collections:
                    processed_collections.add(collection_id)

                    collection_thread = ThreadWithReturnValue(
                        target=get_resource_from_rlef,
                        args=(user_id, task_id, collection_id, "sync", chatbot_name)
                    )
                    collection_threads.append(collection_thread)
                    collection_thread.start()

                    if len(collection_threads) >= max_threads:
                        finished_thread = collection_threads.pop(0)
                        batch_summary, file_summary, rlef_data, total_file_map_count = finished_thread.join()
                        total_count += total_file_map_count

                        if batch_summary is not None:
                            batch_summary_list.extend(batch_summary if isinstance(batch_summary, list) else [batch_summary])
                        if file_summary is not None:
                            file_summary_list.extend(file_summary if isinstance(file_summary, list) else [file_summary])
                        if rlef_data is not None:
                            rlef_data_list.extend(rlef_data if isinstance(rlef_data, list) else [rlef_data])

            for finished_thread in collection_threads:
                batch_summary, file_summary, rlef_data, total_file_map_count = finished_thread.join()
                total_count += total_file_map_count

                if batch_summary is not None:
                    batch_summary_list.extend(batch_summary if isinstance(batch_summary, list) else [batch_summary])
                if file_summary is not None:
                    file_summary_list.extend(file_summary if isinstance(file_summary, list) else [file_summary])
                if rlef_data is not None:
                    rlef_data_list.extend(rlef_data if isinstance(rlef_data, list) else [rlef_data])

        except Exception as e:
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FILEMAP_INGESTION.value,
                status=PossibleStatuses.WARNING.value,
                count=0,
                count_add_up=True,
                error=f"Error in sync_rlef_file_summaries: {traceback.format_exc()}"
            )
            error_logger.error(f"Error in sync_rlef_file_summaries: {e}")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILEMAP_INGESTION.value,
            status=PossibleStatuses.COMPLETED.value,
            count=total_count
        )

        updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list = handle_old_rlef_summaries(initial_data['dashboard']['rlef_data'], rlef_data_list)
    else:
        updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list = fetch_repository_summaries(user_id, task_id, collections_ids,chatbot_name=chatbot_name)

        if not updated_batch_summary_list or not updated_file_summary_list or not updated_rlef_data_list:
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FETCHING_SUMMARY.value,
                status=PossibleStatuses.FAILED.value,
                error="Something failed in fetch_repo_summaries"
            )
            raise Exception("Something failed in fetch_repo_summaries")
        else:
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FETCHING_SUMMARY.value,
                status=PossibleStatuses.COMPLETED.value
            )

    update_batch_summaries_in_db(user_id, task_id, updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list)

    return updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list

def handle_old_rlef_summaries(intial_rlef_data, new_rlfe_data):

    updated_batch_summary_list = []
    updated_file_summary_list = []
    updated_rlef_data_list = []
    unchanged_batch, updated_batch, new_batch = [], [], []
    try:
        # info_logger.info(f"Initial RLEF Data: {intial_rlef_data}\n\n")
        # info_logger.info(f"New RLEF Data: {new_rlfe_data}\n\n\n")

        unchanged_batch, updated_batch, new_batch = filter_and_update_batches(intial_rlef_data, new_rlfe_data)

        # info_logger.info(f"Unchanged Batch: {unchanged_batch}")
        # info_logger.info(f"Updated Batch: {updated_batch}")
        # info_logger.info(f"New Batch: {new_batch}")

        new_rlef_data = updated_batch + new_batch

        updated_rlef_data = process_batch_data(new_rlef_data)

        updated_rlef_data_list = updated_rlef_data + unchanged_batch


        updated_batch_summary_list = [entry.get("batch_summary") for entry in updated_rlef_data_list]
        updated_file_summary_list = [item for entry in updated_rlef_data_list for item in entry.get("batch_data", [])]

        # info_logger.info(f"Updated RLEF Data: {updated_rlef_data_list}")
        # info_logger.info(f"Updated Batch Summary List: {updated_batch_summary_list}")
        # info_logger.info(f"Updated File Summary List: {updated_file_summary_list}")

        return updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list
    except Exception as e:
        print(f"Error in handle_old_rlef_summaries: {e}")

def filter_and_update_batches(old_batches, new_batches):
    updated_batches = []
    absolute_old_batches = []

    try:
        for old_batch in old_batches:
            updated = False
            for new_batch in new_batches:
                for new_file in new_batch.get("batch_data", []):
                    for old_file in old_batch.get("batch_data", []):
                        if old_file.get("file_path") == new_file.get("file_path"):
                            old_file["file_summary"] = new_file.get("file_summary","")
                            old_file["update_type"] = new_file.get("update_type","")
                            old_file["git_url"] = new_file.get("git_url", "")
                            updated = True
                            new_batch["batch_data"].remove(new_file)

            if updated:
                updated_batches.append(old_batch)
            else:
                absolute_old_batches.append(old_batch)
    except Exception as e:
        print(f"An error occurred: {e}")

    return absolute_old_batches, updated_batches, new_batches



def check_commit_changes(repos_data: List[Dict], allowed_extensions: Dict[str, List[str]] = None) -> Tuple[bool, List[Dict], List[str]]:
    """
    Process multiple repositories and check for changes, validating file extensions.

    Args:
        repos_data (List[Dict]): List of dictionaries containing repository information
            Each dict should have:
            - url: repository URL
            - branch_name: branch to check
            - access_token: GitHub PAT token
            - commit_hash: old commit hash to compare against
        allowed_extensions (Dict[str, List[str]]): Dictionary of allowed file extensions by language

    Returns:
        Tuple[bool, List[Dict], List[str]]:
            - Boolean indicating if any repo has valid changes
            - List of dictionaries containing results for each repo
            - List of commit IDs
    """
    any_changes = False
    results = []
    commit_ids = []
    ALLOWED_EGPT_SUMMARY_EXTENSIONS, STATUS = get_supported_languages_dict()

    all_allowed_extensions = set()
    if allowed_extensions:
        for extensions in allowed_extensions.values():
            all_allowed_extensions.update(extensions)

    for repo in repos_data:
        has_changes, repo_result = check_repository_changes(
            repo_url=repo['url'],
            branch_name=repo['branch_name'],
            old_commit_id=repo['commit_hash'],
            pat_token=repo['access_token']
        )

        repo_result['supported_changes'] = False
        
        if has_changes and allowed_extensions:
            for file_path in repo_result['changed_files']:
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext in all_allowed_extensions:
                    repo_result['supported_changes'] = True
                    any_changes = True
                    break
        elif has_changes:
            repo_result['supported_changes'] = True
            any_changes = True

        results.append(repo_result)
        commit_ids.append(repo_result['commit_id'])

    return any_changes, results, commit_ids


def _store_pre_sync_data(request_data_recieved, old_data, new_task_id, user_id, number_of_files, number_of_tokens, github_token, user_data, active_user_id):
    """
    Store pre sync data
    """
    try:
        owner_email, owner_name = fetch_user_email(user_id, user_data)
        active_user_email, active_user_name = fetch_user_email(active_user_id, user_data)
        request_data_recieved["github_token"] = encrypting_token(github_token)
        request_data_recieved['git_url'] = old_data.get('git_url', [])
        request_data_recieved['branch_names'] = old_data.get('branch_names', [])
        request_data_recieved['github_info'] = old_data.get('git_info', [])
        request_data_recieved['analysis_type'] = "sync"
        
        pre_sync_data = {
            "task_id": new_task_id,
            "user_id": user_id,
            "email_id": active_user_email,
            "user_name": active_user_name,
            "project_name": old_data.get("project_name", ""),
            "analysis_type": "sync",
            "batch_size": old_data.get("batch_size", 0),
            "project_id": old_data.get("project_id", ""),
            "created_at": str(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
            "updated_at": str(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
            "pre_ingestion_data": {
                "request_data": request_data_recieved,
                "user_name": active_user_name,
                "user_email": active_user_email,
                "version": old_data.get("version", 0),
                "project_name": old_data.get("project_name", ""),
                "description": old_data.get("description", ""),
                "analysis_type": "sync",
                "number_of_files": number_of_files,
                "number_of_tokens": number_of_tokens,
                "estimated_cost" : old_data.get("approx_output_cost", 0),
                "project_id": old_data.get("project_id", ""),
                "status": "Pending",
                "created_at": str(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
                "updated_at": str(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
            }
        }

        # Insert into DB
        result = project_summary_db.project_analyzer_task.insert_one(pre_sync_data)
        if result.inserted_id:
            info_logger.info(f"Pre sync data stored for task_id: {new_task_id}")

        return True
    except Exception as e:
        print(f"Error in _store_pre_sync_data: {e}")
        return False
