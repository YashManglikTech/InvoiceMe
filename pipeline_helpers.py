from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import ast
import os
import re
import subprocess
import threading
import time, traceback
from typing import List, Optional
import uuid
from analyser_repo_pipeline.alloydb_helpers import AlloyDBConnector
from bson import ObjectId
from pymongo.collection import Collection
from json_repair import repair_json
import requests
from EGPT_extract_ingest_files import exe_summary_prompt, feature_hierarchy_prompt, feature_summary_prompt
from absolute_path.language_support import get_supported_languages_dict
from ai_suggestions_promptexamples import get_dynamic_services_to_reactflow_prompts_v2
from analyser_repo_pipeline.llm_service.prompts import architecture_diagram_generation, services_generation, executive_summary_generation, feature_summary_generation
from analyser_repo_pipeline.llm_service.llm_call_utils import generate_llm_response
from analyser_repo_pipeline.egpt_helpers import login_to_egpt, upload_repo_egpt, upload_repo_egpt_mongo
from analyser_repo_pipeline.feature_hierarchy_generation_pipeline.FHPostProcessing import add_misc_files_to_ft_hierarchy, post_processing
from analyser_repo_pipeline.feature_hierarchy_generation_pipeline.feature_hierarchy_post import generate_final_feature_hierarchy, mapping_new_names_after_clustering, merge_features, normalize_confidence_scores, process_features, reduce_number_of_features, reduce_number_of_sub_features, merging_similar_features, save_embeddings
from analyser_repo_pipeline.langauge_helpers import get_repo_languages
from analyser_repo_pipeline.rlef_helpers import get_resource_from_rlef, upload_dependency_rlef_get_api, upload_txt_autoai
from analyser_repo_pipeline.vulnerability_check import get_custome_cs_violations
from folder_summary_generation.pipeline import generate_folder_summaries_pipeline
from helper_func_2 import get_chatbot_id
from repo_services_2 import extract_response_from_delimeters
from clone_repo import clone_github_repo, get_current_commit_hash
import config
from crashanalytics import log_into_bigquery
from db_update import save_to_ecg_db, storing_commit_hashes_in_db, storing_repowise_summary_in_db, success_update_repo_languages_in_db, update_database_with_rlef_ids_and_chatbot_name, update_dependecy_language_support_in_db, update_executive_summary_in_db, update_feature_hierarchyhierarchy_in_db, update_feature_summary_in_db, update_folder_summaries_in_db, update_project_services_completion_status_in_db, update_v2_archcompletion_status_in_db
from generalised_utils import count_lines, create_repo_folder, remove_repo_folder
from helper_func import get_icons_metadata, project_exec_call
from new_architecture_diagram_from_llm import anthropic_service_reactflow_api_call
from absolute_path.new_parser import generate_dependencies_pipeline_parallel
from predict import gemini_call_api, get_ai_response_predict_api_refactoringcode, gemini_call_flash_2
from repo_services import anthropic_api_call,system_prompt, combined_external_thirdparty_prompt
from vertexai.generative_models import GenerativeModel
from models import project_summary_db, egpt_agents_collection
from logger import info_logger, error_logger, warning_logger, debug_logger
from analyser_repo_pipeline.agent_config import setup_rag_agent_config, setup_appmodai_agent_config
from analyser_repo_pipeline.agent_prompts import agent_settings_additional_points, agent_settings_orchestrator_prompt, contextaware_tool_settings_orchestrator_prompt
from analyser_repo_pipeline.agent_helper import get_model_type
from reorder_based_on_feature_summary import reorder_feature_hierarchy
from analyser_repo_pipeline.design_pattern_generation import generate_design_pattern
from analysis_status_update import update_step_status, update_task_fields
from utils.analysis_status_enums import StepStatus, PossibleStatuses
import psycopg2

class ThreadWithReturnValue(threading.Thread):

  def __init__(self, group=None, target=None, name=None,
               args=(), kwargs={}, Verbose=None):
    threading.Thread.__init__(self, group, target, name, args, kwargs)
    self._return = None

  def run(self):
    if self._target is not None:
      self._return = self._target(*self._args, **self._kwargs)

  def join(self, *args):
    threading.Thread.join(self, *args)
    return self._return

def initialize_project(task_id):
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.PROJECT_INITILIZED.value,
        status=PossibleStatuses.IN_PROGRESS.value
    )
    try:
        repo_folder = f"Repos_{task_id}"
        if os.path.exists(repo_folder):
            remove_repo_folder(repo_folder)
        create_repo_folder(repo_folder)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.PROJECT_INITILIZED.value,
            status=PossibleStatuses.COMPLETED.value
        )
        return repo_folder
    except Exception as e:
        error = f"Error initializing project for task ID {task_id}: {traceback.format_exc()}"
        error_logger.error(error)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.PROJECT_INITILIZED.value,
            status=PossibleStatuses.FAILED.value,
            error=error
        )
        raise e



def collect_task_ids(task_id:str, v:List, collection:Collection):
    if not task_id: return
    v.append(task_id)
    cursor = collection.find_one({"task_id":task_id})
    if cursor:
        return collect_task_ids(cursor.get("parent_id"), v, collection)
    
def count_files_excluding_hidden_dirs(root_dir):
    total_files = 0
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if os.path.basename(dirpath) == '.git' or os.path.basename(dirpath) == '.github':
            # Skip this dir and its files
            continue
        # Filter subdirectories to avoid walking into them
        dirnames[:] = [d for d in dirnames if not (d == '.git' or d == '.github')]
        
        # info_logger.info(f"Visible files in {dirpath}: {len(visible_files)}")
        # info_logger.info(f"Running total before this folder: {total_files}")
        
        total_files += len(filenames)
        
    return total_files


def clone_repositories(github_urls, branch_names, git_pat_token, repo_folder, user_id, task_id):
    local_repo_paths = []
    try:
        info_logger.info(f"Cloning repositories for task ID: {task_id}")
        clone_threads = []
        clone_errors = []    
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.CLONING.value,
            status=PossibleStatuses.IN_PROGRESS.value,
            count=0
        )
        for github_url, branch_name in zip(github_urls, branch_names):
            try:
                if not github_url.endswith('.git'):
                    github_url = f"{github_url}.git"
                complete_repo_url = f"https://{git_pat_token}@{github_url.split('https://')[1]}"
                clone_thread = ThreadWithReturnValue(target=clone_github_repo, 
                                                    args=(complete_repo_url, repo_folder, branch_name))
                clone_threads.append(clone_thread)
                clone_thread.start()
            except Exception as e:
                error_msg = f"Failed to start clone thread for {github_url}: {traceback.format_exc()}"
                clone_errors.append(error_msg)
                log_into_bigquery("clone_repositories", user_id, task_id, error_msg, 500)
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.CLONING.value,
                    status=PossibleStatuses.FAILED.value,
                    error=error_msg,
                    count=0
                )
        
        for thread in clone_threads:
            result = thread.join()
            if result is not None:
                local_repo_paths.append(result)
            else:
                clone_errors.append(f"Cloning failed for a repository")
        if not local_repo_paths:
            error_msg = "All repository clones failed. " + "; ".join(clone_errors)
            log_into_bigquery("clone_repositories", user_id, task_id, error_msg, 500)
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.CLONING.value,
                status=PossibleStatuses.FAILED.value,
                error=error_msg,
                count=0
            )
            raise Exception(error_msg)
        else:
            total_file_count = sum(count_files_excluding_hidden_dirs(repo_path) for repo_path in local_repo_paths)
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.CLONING.value,
                status=PossibleStatuses.COMPLETED.value,
                count=len(local_repo_paths)
            )
            update_task_fields(
                task_id=task_id,
                total_files=total_file_count
            )
        return clone_threads
    except Exception as e:
        error_msg = f"Critical error in cloning repositories: {traceback.format_exc()}"
        error_logger.error(error_msg)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.CLONING.value,
            status=PossibleStatuses.FAILED.value,
            count=len(local_repo_paths),
            error=error_msg
        )
        log_into_bigquery("clone_repositories", user_id, task_id, error_msg, 500)
        raise Exception(error_msg)


def handle_repo_commit_hashes(local_repo_paths, user_id, task_id, github_info_list):
    info_logger.info(f"Getting commit hashes for task ID: {task_id}")
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.COMMIT_HASH_CHECK.value,
        status=PossibleStatuses.IN_PROGRESS.value,        
    )
    try:
        commit_hashes = [get_current_commit_hash(repo_path) for repo_path in local_repo_paths]

        storing_commit_hashes_in_db(task_id=task_id, commit_hashes=commit_hashes, github_info=github_info_list)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.COMMIT_HASH_CHECK.value,
            status=PossibleStatuses.COMPLETED.value
        )
        return commit_hashes
    except Exception as e:
        error_msg = f"Error in getting commit hashes: {str(e)}"
        log_into_bigquery("handle_repo_commit_hashes", user_id, task_id, error_msg, 500)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.COMMIT_HASH_CHECK.value,
            status=PossibleStatuses.FAILED.value,
            error=error_msg
        )
        raise e
            
            
def upload_repositories(task_id, github_urls, git_pat_token, branch_names, commit_hashes, egpt_token, project_id, batch_size, ingestion_model_type="Google VertexAI", ingestion_model_name="gemini-2.0-flash"):
    # Upload the repositories to EGPT using threading
    info_logger.info("Uploading repositories to EGPT")
    upload_threads = []
    for github_url, branch_name, commit_hash in zip(github_urls, branch_names, commit_hashes):
        complete_repo_url = f"https://{git_pat_token}@{github_url.split('https://')[1]}"
        upload_thread = ThreadWithReturnValue(target=upload_repo_egpt, args=(task_id, complete_repo_url, egpt_token, project_id, batch_size, branch_name, commit_hash, ingestion_model_type, ingestion_model_name))
        upload_threads.append(upload_thread)
        upload_thread.start()
    return upload_threads

def convert_github_url(repo_url):
    """
    Convert GitHub SSH URL to HTTPS format if SSH format is provided.
    Otherwise, return the original URL.
    
    Args:
        repo_url (str): GitHub repository URL in either SSH or HTTPS format
        
    Returns:
        str: GitHub repository URL in HTTPS format
        
    Examples:
        >>> convert_github_url("git@github.com:owner/repo.git")
        "https://github.com/owner/repo.git"
        >>> convert_github_url("https://github.com/owner/repo.git")
        "https://github.com/owner/repo.git"
    """
    # Check if it's an SSH URL
    if repo_url.startswith("git@github.com:"):
        # Extract the repository path after 'git@github.com:'
        repo_path = repo_url.split('git@github.com:')[1]
        # Convert to HTTPS format
        return f"https://github.com/{repo_path}"
    
    return repo_url

def get_repository_languages(task_id, user_id, github_urls, git_pat_token):
    info_logger.info(f"Getting repository languages for task ID: {task_id}")
    try:
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.LANGUAGE.value,
            status=PossibleStatuses.IN_PROGRESS.value,
        )

        error_messages = []
        repo_language_list = []
        successful_languages = []
        for github_url in github_urls:
            try:
                github_url = convert_github_url(github_url)
                languages, status = get_repo_languages(github_url, user_id, task_id, git_pat_token)

                if status == 200:
                    repo_language_list.append(languages)
                    successful_languages.append(languages)
                else:
                    error_msg = f"Language fetch failed for {github_url}, Status: {status}"
                    error_messages.append(error_msg)
                    log_into_bigquery("get_repository_languages", user_id, task_id, error_msg, status)
                    repo_language_list.append({})

            except Exception as e:
                error_msg = f"Exception processing {github_url}: {str(e)}"
                error_messages.append(error_msg)
                log_into_bigquery("get_repository_languages", user_id, task_id, error_msg, 500)
                repo_language_list.append({})

        if error_messages:
            error_logger.error("Partial language retrieval errors:", "\n".join(error_messages))

        if successful_languages:
            try:
                result = check_dependecy_graph_support(successful_languages)
                # print(f"Unsupported Languages: {result.get('unsupported_languages', [])}")
                # print(f"Status: {result.get('status')}")
                # print(f"Sort Type: {result.get('sort_type')}")

                update_dependecy_language_support_in_db(task_id, result)
            except Exception as dep_check_error:
                error_msg = f"Dependency graph support check failed: {str(dep_check_error)}"
                log_into_bigquery("get_repository_languages", user_id, task_id, error_msg, 500)

        combined_error_message = "\n".join(error_messages)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.LANGUAGE.value,
            status=PossibleStatuses.FAILED.value if combined_error_message else PossibleStatuses.COMPLETED.value,
            error=combined_error_message.strip()
        )
        success_update_repo_languages_in_db(task_id, github_urls, repo_language_list, combined_error_message.strip())

        if not successful_languages:
            raise Exception("No repository languages could be retrieved")

        return repo_language_list
    
    except Exception as e:
        error_msg = f"Critical error in repository language retrieval: {traceback.format_exc()}"
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.LANGUAGE.value,
            status=PossibleStatuses.FAILED.value,
            error=error_msg
        )
        log_into_bigquery("get_repository_languages", user_id, task_id, error_msg, 500)
        raise e


def check_dependecy_graph_support(repo_language_list):

    ALLOWED_EGPT_SUMMARY_EXTENSIONS, STATUS = get_supported_languages_dict()
    ALLOWED_DEPENDENCY_GRAPH_LANGUAGES = [key.lower() for key in ALLOWED_EGPT_SUMMARY_EXTENSIONS.keys()]
 
    allowed_languages = [lang.lower() for lang in ALLOWED_DEPENDENCY_GRAPH_LANGUAGES]
    unsupported_languages = set()

    sorted_languages = sorted(
        [(language, count) for repo_languages in repo_language_list for language, count in repo_languages.items()],
        key=lambda x: x[1],
        reverse=True
    )

    for language, count in sorted_languages:
        if language.lower() not in allowed_languages:
            unsupported_languages.add(language)

    return {
        "unsupported_languages": list(unsupported_languages),
        "sort_type": "descending",
        "status": len(unsupported_languages) == 0
    }


def handle_mongo_upload(user_id ,task_id, chatbot_name, project_name, github_urls):
    mongo_threads = []
    mongo_thread = ThreadWithReturnValue(target=upload_repo_egpt_mongo, args=(user_id, task_id,chatbot_name, github_urls, ))
    mongo_threads.append(mongo_thread)
    mongo_thread.start()

    mongo_results = []
    for mongo_thread in mongo_threads:
        success, rlef_ids = mongo_thread.join()
        if success:
            info_logger.info("Repo Ingestion Successful")
            # print("Mongo Upload Successful. RLEF IDs:", rlef_ids)
            mongo_results.extend(rlef_ids)
        else:
            error_logger.error("Mongo Upload Failed or Chatbot not found.")
            log_into_bigquery("Mongo Upload", user_id, task_id, "Chatbot not found", 404)
    
    update_database_with_rlef_ids_and_chatbot_name(task_id, rlef_ids, project_name)
    return mongo_results



def perform_vulnerability_checks(user_id, task_id,local_repo_paths, github_urls):
    vulnerability_threads = []
    for local_path, github_url in zip(local_repo_paths, github_urls):
        clean_local_path = os.path.normpath(local_path)
        vulnerability_thread = ThreadWithReturnValue(target=get_custome_cs_violations, args=(clean_local_path, github_url, task_id, user_id,))
        vulnerability_threads.append(vulnerability_thread)
        vulnerability_thread.start()

    # update_vulnerability_check_completion_status_in_db(task_id)
    return vulnerability_threads

def create_filemap_table(task_id, chatbot_name, max_retries=3, retry_delay=2):
    table_name = f"{config.ORGANIZATION_NAME}-{chatbot_name}-filemap"
    attempt = 0
    while attempt < max_retries:
        try:
            alloydb_connector = AlloyDBConnector(
                database=config.EGPT_ALLOY_DB_NAME,
                username=config.EGPT_ALLOY_DB_USERNAME,
                password=config.EGPT_ALLOY_DB_PASSWORD,
                host=config.EGPT_ALLOY_DB_HOST,
                port=config.EGPT_ALLOY_DB_PORT
            )
            create_table_query = f"""CREATE TABLE IF NOT EXISTS "{table_name}" (
                                        embed_id UUID PRIMARY KEY,
                                        rlef_resource_id TEXT,
                                        file_path TEXT,
                                        user_id TEXT,
                                        task_id TEXT,
                                        chunk_type TEXT
                                    );"""
            info_logger.info(f'Creating the mapping table for file_path and embedding id if not already exists with table name: {table_name}')
            alloydb_connector.run(
                query=create_table_query,
                query_type="create"
            )
            return  # Exit the function if successful
        except psycopg2.Error as e:
            error_logger.error(f"Database error occurred: {traceback.format_exc()}")
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FILEMAP_INGESTION.value,
                status=PossibleStatuses.WARNING.value,
                error=f"Database error occurred: {traceback.format_exc()}, retrying..."
            )
        except Exception as e:
            error_logger.error(f"Unexpected error occurred while creating the table: {traceback.format_exc()}")
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FILEMAP_INGESTION.value,
                status=PossibleStatuses.WARNING.value,
                error=f"Unexpected error occurred while creating the table: {traceback.format_exc()}, retrying..."
            )
        
        attempt += 1
        info_logger.info(f"Retrying to create the table ({attempt}/{max_retries}), delaying for {retry_delay} seconds...")
        time.sleep(retry_delay)
    
    error_logger.error(f"Failed to create the table after {max_retries} attempts.")
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FILEMAP_INGESTION.value,
        status=PossibleStatuses.FAILED.value,
        error=f"Failed to create the table after {max_retries} attempts."
    )

def fetch_repository_summaries(user_id, task_id, rlef_ids, max_threads=25, chatbot_name=""):
    batch_summary_list = []
    repo_summary_threads = []
    file_summary_list = []
    rlef_data_list = [] 
    processed_rlef_ids = set()

    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FETCHING_SUMMARY.value,
        status=PossibleStatuses.IN_PROGRESS.value,
    )
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FILEMAP_INGESTION.value,
        status=PossibleStatuses.IN_PROGRESS.value,
        count=0
    )
    create_filemap_table(
        task_id=task_id,
        chatbot_name=chatbot_name
    )
    total_count = 0
    for rlef_id in rlef_ids:
        if rlef_id is not None and rlef_id not in processed_rlef_ids:
            info_logger.info(f"Processing RLEF ID: {rlef_id}")
            processed_rlef_ids.add(rlef_id)
            request_type = "complete"
            repo_summary_thread = ThreadWithReturnValue(target=get_resource_from_rlef, args=(user_id, task_id, rlef_id, request_type, chatbot_name,))
            repo_summary_threads.append(repo_summary_thread)
            repo_summary_thread.start()

            if len(repo_summary_threads) >= max_threads:
                finished_thread = repo_summary_threads.pop(0)
                batch_summary, file_summaries, batch_data, total_file_map_count = finished_thread.join()
                total_count += total_file_map_count

                if file_summaries is not None or batch_summary is not None:
                    batch_summary_list.append(batch_summary)

                    try:
                        json_res = json.loads(file_summaries)
                        file_summary_list.extend(json_res)
                        
                        if batch_data:
                            rlef_data_list.append(batch_data)
                    except Exception as e:
                        print(f"Error in parsing JSON file_summaries: {e}")
                        update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.FETCHING_SUMMARY.value,
                            status=PossibleStatuses.WARNING.value,
                            error=f"Error in parsing JSON file_summaries: {e} for rlef_id {rlef_id}"
                        )

                        file_summary_list.append(" ")
                else:
                    print("Error: Unable to fetch repository summary")
                    update_step_status(
                        task_id=task_id,
                        step_name=StepStatus.FETCHING_SUMMARY.value,
                        status=PossibleStatuses.WARNING.value,
                        error=f"Error: Unable to fetch repository summary for rlef_id {rlef_id}"
                    )
                    log_into_bigquery("fetch_repo_summaries", user_id, task_id, "Unable to fetch repository summary", 400)
        else:
            print("rlef_id is None or already processed")
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FETCHING_SUMMARY.value,
                status=PossibleStatuses.WARNING.value,
                error=f"rlef_id is None or already processed for rlef_id {rlef_id}"
            )

    for finished_thread in repo_summary_threads:
        batch_summary, file_summaries, batch_data, total_file_map_count = finished_thread.join()  # Modified to receive batch_data
        total_count += total_file_map_count

        if file_summaries is not None or batch_summary is not None:
            # print("*" * 100)
            # print("\n\nFinal Summary --- ", batch_summary)
            # print("Summary list --- ", file_summaries)
            # print("*" * 100)

            batch_summary_list.append(batch_summary)

            try:
                json_res = json.loads(file_summaries)
                file_summary_list.extend(json_res)
                
                if batch_data:
                    rlef_data_list.append(batch_data)
            except Exception as e:
                print(f"Error in parsing JSON response: {e}")
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.FETCHING_SUMMARY.value,
                    status=PossibleStatuses.WARNING.value,
                    error=f"Error in parsing JSON response in fetch_repository_summaries: {e}"
                )
                file_summary_list.append(" ")
        else:
            log_into_bigquery("fetch_repo_summaries", user_id, task_id, "Unable to fetch repository summary", 400)
            print("Error: Unable to fetch repository summary")
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FETCHING_SUMMARY.value,
                status=PossibleStatuses.WARNING.value,
                error=f"Error: Unable to fetch repository summary"
            )
            
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FILEMAP_INGESTION.value,
        status=PossibleStatuses.COMPLETED.value,
        count=total_count
    )

    combined_data = defaultdict(lambda: {"git_url": "", "file": "", "summary": "", "is_chunked": False})


    for entry in file_summary_list:
        file_path = entry.get("file")
        if not file_path:
            continue  

        if file_path not in combined_data:
            combined_data[file_path] = {} 

        combined_data[file_path]["git_url"] = entry.get("git_url", "")
        combined_data[file_path]["update_type"] = entry.get("update_type", "")
        combined_data[file_path]["file"] = file_path
        combined_data[file_path]["is_chunked"] = True

        summary = combined_data[file_path].get("summary", "")
        services_names  = combined_data[file_path].get("service_names", "")
        
        entry_summary = entry.get("summary", "")
        entry_services_names = entry.get("service_names", "")
        
        combined_data[file_path]["summary"] = f"{entry_summary}\{summary}" if summary else entry_summary
        combined_data[file_path]["service_names"] = f"{entry_services_names}\n{services_names}" if services_names else entry_services_names
        
        
    updated_file_summary_list = list(combined_data.values())


    return batch_summary_list, updated_file_summary_list, rlef_data_list


def fetch_and_map_summaries(dependency_tree: dict, file_summaries: list, repo_url: str, git_pat_token:str):
    """
    Map file summaries to dependency tree paths with enhanced condition tracing.
    
    Args:
        dependency_tree (dict): The dependency tree structure to update
        file_summaries (list): List of file-by-file summaries
        repo_url (str): Repository URL to match with file summaries
        git_pat_token (str): Git Personal Access Token for URL authentication
                
    Returns:
        dict: Updated dependency tree with mapped summaries
    """
    try:
        if not dependency_tree or not isinstance(dependency_tree, dict):
            error_logger.error("Error: Invalid dependency tree structure")
            return dependency_tree
            
        parts = repo_url.replace("https://github.com/", "").rstrip(".git").split("/")
        owner, repo = parts[0], parts[1]

        complete_repo_url = f"https://{git_pat_token}@{repo_url.split('https://')[1]}"
        
        try:
            summary_map = {}
            services_map = {}
            
            for summary in file_summaries:
                condition_results = {
                    'git_url_match': summary.get('git_url') == complete_repo_url,
                    'file_exists': bool(summary.get('file')),
                    'summary_exists': bool(summary.get('summary')),
                    'file_not_end': summary.get('file') != "end"
                }
                
                failed_conditions = [
                    key for key, value in condition_results.items() if not value
                ]
                
                if all(condition_results.values()):
                    file_path = summary['file']
                    if not file_path.startswith(repo + "/"):
                        file_path = repo + "/" + file_path
                    summary_map[file_path] = summary['summary']
                    services_map[file_path] = summary.get('service_names', "")
                    info_logger.info(f"Matched: {file_path}")
                else:
                    info_logger.info(f"Summary skipped. Failed conditions: {failed_conditions}")
            
            if "files" not in dependency_tree:
                return dependency_tree

            for file in dependency_tree["files"]:
                file_path = file.get("path")
                if file_path:
                    if file_path in summary_map:
                        file["summary"] = summary_map[file_path]
                        file["service_names"] = services_map[file_path]
                    else:
                        repo_prefixed_path = repo + "/" + file_path if not file_path.startswith(repo + "/") else file_path
                        if repo_prefixed_path in summary_map:
                            file["summary"] = summary_map[repo_prefixed_path]
                            file["service_names"] = services_map[repo_prefixed_path]
                        else:
                            for stored_path, summary in summary_map.items():
                                if stored_path.endswith(file_path):
                                    file["summary"] = summary
                                    file["service_names"] = services_map[stored_path]
                                    break
                            else:
                                file["summary"] = ""
            
            return dependency_tree
            
        except Exception as db_error:
            error_logger.error(f"Database error while fetching summaries: {str(db_error)}")
            return dependency_tree
            
    except Exception as e:
        error_logger.error(f"Unexpected error in fetch_and_map_summaries: {str(e)}")
        return dependency_tree
    

    
def generate_dependencies(task_id: str, git_pat_token:str,repo_urls: list, local_repo_paths: list, languages_lists: list, file_by_file_summaries: list):
    """
    Generate dependencies for repositories based on their local paths and languages.
    
    Args:
        task_id (str): Task ID
        repo_urls (list): List of repository URLs
        local_repo_paths (list): List of local repository paths
        languages_lists (list): List of dictionaries containing language information
                              e.g. [{"Python": 1000, "JavaScript": 500}, {"Go": 300}]
        file_by_file_summaries (list): List of file-by-file summaries for each repository
    
    Returns:
        tuple: (dependancy_ref, dependancy_graph, status_code)
               - dependancy_ref (dict): Reference dictionary from dependency data
               - dependancy_graph (dict): Generated dependency graph data
               - status_code (int): HTTP status code (200 for success, others for failure)
    """
    try:
        results = []
        resource_ref = {}
        feature_view_loc = {}
        
        for repo_url, local_path, language_dict in zip(repo_urls, local_repo_paths, languages_lists):
            # info_logger.info(f"Local Path: {local_path}")
            # info_logger.info(f"Languages: {language_dict}")
            # info_logger.info(f"Repo URL: {repo_url}")
            languages = [key.lower() for key in language_dict.keys()]
            
            generated_dependency_response = generate_dependencies_pipeline_parallel(local_path, languages)
            # info_logger.info(f"Generated Dependency Response: {generated_dependency_response}")
            before_summary = {"files": generated_dependency_response}

                
            after_summary = fetch_and_map_summaries(
                dependency_tree=before_summary, 
                file_summaries=file_by_file_summaries, 
                repo_url=repo_url, 
                git_pat_token=git_pat_token
            )
            
            # with open("after_summary.json", "w") as file:
            #     json.dump(after_summary, file, indent=4)
            
            
            upload_dependency_model_id = config.UPLOAD_DEPENDENCY_MODEL_ID
            autoai_response = upload_dependency_rlef_get_api(
                file_name=f"{task_id}.txt",
                file_content=json.dumps(after_summary),
                model_id=upload_dependency_model_id,
                labels="predicted",
                tag="graph",
                csv="None",
                prompt=None,
                confidence_score=100
            )
            info_logger.info(f"Info: Uploaded dependency data to AutoAI for URL {repo_url} with response: {autoai_response}")

            autoai_response_id = json.loads(autoai_response).get('_id')
            if not autoai_response_id:
                error_logger.error(f"Error: Error uploading to autoai for URL {repo_url}")
                raise Exception("Error uploading to autoai")

            resource_ref[repo_url] = str(autoai_response_id)
            info_logger.info(f"Info: AutoAI response ID for URL {repo_url}: {autoai_response_id}")

            for file in after_summary["files"]:
                feature_view_loc[file["path"]] = count_lines(file["content"])
            
            results.append(generated_dependency_response)
        
        project_summary_db.projects.update_one(
            {"task_id": task_id},
            {'$set': {"dependency_ref": resource_ref, "feature_view_loc": feature_view_loc}}
        )
        info_logger.info(f"Info: Dependencies and feature view locations updated in the database for task ID {task_id}")

        
        return resource_ref, after_summary, 200

    except Exception as e:
        error_msg = f"Error in generate_dependencies: {e} Traceback: {traceback.format_exc()}"
        error_logger.error(error_msg)
        return {}, {"error": error_msg}, 400  
        
        

def upload_dependencies(user_id ,task_id, git_pat_token, rlef_ids):
    # return "","",400
    upload_dependancy_url = f"{config.APPMOD_DOMAIN}/api/github/upload_dependencies"
    upload_dependancy_url_payload = json.dumps({
        "taskid": task_id,
        "token": git_pat_token,
        "collection_id": rlef_ids
    })
    upload_dependancy_url_headers = {'Content-Type': 'application/json'}

    upload_dependancy_response = requests.post(upload_dependancy_url, headers=upload_dependancy_url_headers, data=upload_dependancy_url_payload)
    if upload_dependancy_response.status_code == 200:
        dependancy_ref = upload_dependancy_response.json().get("dependency_ref")
        dependancy_graph = upload_dependancy_response.json().get("dependency_graph")
        print(f"Depedency_graph: {dependancy_graph}")
        print(f"Depedency_ref : {dependancy_ref}")
        status_code = 200
    else:
        print(f"Dependency response : {upload_dependancy_response.text}")
        dependancy_ref = ""
        dependancy_graph = str(upload_dependancy_response.text)
        print("Error : Upload_Dependency API Failed")
        status_code = upload_dependancy_response.status_code
        log_into_bigquery("upload_dependencies", user_id , task_id, "Github PAT Invalid Token", 404)


    return dependancy_ref, dependancy_graph, status_code


def get_file_paths(github_urls, dependancy_ref):
    file_paths = []
    for repo in github_urls:
        paths = get_repo_files(repo, dependancy_ref)
        if paths:
            file_paths.extend(paths)

    return file_paths



def generate_services(user_id, task_id, dependency_graph, file_summary_list, initial_status_code, folder_summary_json, is_folder_mappin=True, user_feedback=None, max_retries=3, model_data:Optional[str]=None):
    """
    Generate services with retry loop and fallback strategy.

    If dependency_graph available (status_code 200) and token count < threshold:
        1. Try Claude with dependency_graph
        2. Try Gemini with dependency_graph
        3. Try Claude with folder/batchwise summary
        4. Try Gemini with folder/batchwise summary

    If dependency_graph not available or token count >= threshold:
        1. Try Claude with folder/batchwise summary
        2. Try Gemini with folder/batchwise summary

    If is_folder_mappin is True:
        Use folder_summary_json for service generation
    Else:
        Use batchwise_summary for service generation
    """
    info_logger.info(f"Generating services for task ID: {task_id}")
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.REPOSITORY_SERVICES.value,
        status=PossibleStatuses.IN_PROGRESS.value
    )

    completion_status = True
    error_messages = []
    model_used = ""
    combined_services_json = {}

    def process_api_response(response, status_code, model_data):
        """Process and validate API response."""
        try:
            if response is not None and status_code == 200:
                formatted_response, format_status_code = format_generated_service_response(response)
                if format_status_code != 200:
                    return None, ""
                    
                formatted_response = json.loads(formatted_response.strip())
                return formatted_response, model_data
            return None, ""
        except Exception as e:
            error_logger.error(f"Error in process_api_response: {e}")
            traceback.print_exc()
            return None, ""

    # Determine which strategies to use based on initial_status_code and token count
    use_dependency_strategies = False
    # initial_status_code = 200
    if initial_status_code == 200:
        # Check token count for dependency graph
        user_prompt = format_dependency_metadata_for_services(file_summary_list)
        
        system_prompt, _ = services_generation()
        messages = [user_feedback] if user_feedback is not None else []

        # Execute strategy based on type

        response = generate_llm_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            messages=messages,
            model_data=model_data,
            user_feedback=user_feedback,
        )
        status_code = 200
        # print("services response : ", response)
        # Process response
        result, model_used = process_api_response(response, status_code, model_data)

        if result:
            combined_services_json = result
            
            # Update database with results
            update_project_services_completion_status_in_db(
                task_id, combined_services_json, completion_status, "", model_used
            )
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.REPOSITORY_SERVICES.value,
                status=PossibleStatuses.COMPLETED.value
            )
            
            return combined_services_json

    # All strategies failed
    error_messages.append("Error: All strategies and retry attempts failed.")
    completion_status = False

    # Update database with failure
    error_message = "; ".join(error_messages)
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.REPOSITORY_SERVICES.value,
        status=PossibleStatuses.FAILED.value,
        error=error_message
    )
    update_project_services_completion_status_in_db(
        task_id, {}, completion_status, error_message, ""
    )

    info_logger.info("Failed to generate services after all attempts")
    return {}




def format_dependency_metadata_for_services(json_data):
    metadata_str = "COMPLETE REPO SERVICES CONTENT WITH DEPENDENCIES\n\n"

    for file in json_data:
        file_path = file.get('git_url',"")
        service_names = file.get('service_names',"")
        summaries = file.get('summary',"")
        metadata_str += f"----\nFile path : ```{file_path}```\n\n"
        if service_names:
            metadata_str += f"Services used in File :\n{service_names}\n\n"
        else:
            metadata_str += f"Summary of serveices used in File :\n{summaries}\n\n"

    metadata_str += "-----\n\n<MUST> Follow the output format : Service JSON must be in between ```json and ``` Delimeters\n\n"

    return metadata_str.strip()




def format_batchwise_summary_metadata_for_services(batch_summarylist):
    metadata_str = "BATCHWISE SUMMARY FOR COMPLETE REPO CONTENT\n\n"

    for idx, summary in enumerate(batch_summarylist, start=1):
        metadata_str += f"Batch {idx} Summary:\n\"\"\"\n{summary}\n\"\"\"\n\n"

    return metadata_str.strip()

def format_folder_summary_metadata_for_services(folder_summarylist):
    """
    Extract folder summaries and services from a nested JSON structure and format as XML.
    
    Args:
        json_data (dict): The JSON data containing folder information
        
    Returns:
        str: XML formatted string with folder summaries and services
    """
    
    
    for data in folder_summarylist:
        git_hub_url = data.get("git_url", "")
        folder_data = data.get("folder_data", {})
        
        result = "<metadata>\n Repo URL: " + git_hub_url + "\n"
        
        def process_folder(folder_data, level=0, indent="  "):
            nonlocal result
            
            if folder_data.get("summary"):
                folder_name = folder_data.get("folder_name", "")
                summary = folder_data.get("summary", "")
                services = folder_data.get("services_names", "")
                folder_path = folder_data.get("folder_path", "")
                
                # Add folder level indicator
                prefix = "-" * level if level > 0 else ""
                folder_display = f"{prefix}{folder_name}" if level > 0 else folder_name
                
                result += f"{indent}<folder level=\"{level}\">\n"
                result += f"{indent}  <name>{folder_display}</name>\n"
                result += f"{indent}  <path>{folder_path}</path>\n"
                result += f"{indent}  <summary>{summary}</summary>\n"
                result += f"{indent}  <services>{services}</services>\n"
                result += f"{indent}</folder>\n"
            
            # if "sub_folders" in folder_data and isinstance(folder_data["sub_folders"], list):
            #     for subfolder in folder_data["sub_folders"]:
            #         process_folder(subfolder, level + 1, indent)
        
        process_folder(folder_data)
        
        result += "</metadata>"
        
        
    return result


def format_top_level_only(folder_summarylist):
    """
    Format only the top-level folder details including name, path, summary, and services.
    
    Args:
        folder_summarylist (list): List of folder summary dictionaries
        
    Returns:
        str: XML formatted string with only top-level data
    """
    result = ""

    for data in folder_summarylist:
        git_hub_url = data.get("git_url", "")
        folder_data = data.get("folder_data", {})

        folder_name = folder_data.get("folder_name", "")
        folder_path = folder_data.get("folder_path", "")
        summary = folder_data.get("summary", "")
        feature_list = folder_data.get("feature_details","")
        services = folder_data.get("services_names", "")

        result += "<metadata>\n"
        result += f"  <repo_url>{git_hub_url}</repo_url>\n"
        result += f"    <name>{folder_name}</name>\n"
        result += f"    <path>{folder_path}</path>\n"
        result += f"    <summary>{summary}</summary>\n"
        result += f"      <feature info>{feature_list}</feature info>"
        result += f"    <services>{services}</services>\n"
        result += "</metadata>\n"

    return result



def format_with_subfolders_services(folder_summarylist):
    """
    Format top-level and subfolder details.
    - Top-level: name, path, summary
    - Subfolders: name, path, services
    
    Args:
        folder_summarylist (list): List of folder summary dictionaries
        
    Returns:
        str: XML formatted string with folder and subfolder details
    """
    result = ""

    for data in folder_summarylist:
        git_hub_url = data.get("git_url", "")
        folder_data = data.get("folder_data", {})

        result += "<metadata>\n"
        result += f"  <repo_url>{git_hub_url}</repo_url>\n"

        def process_folder(folder, level=0, indent="  "):
            nonlocal result

            folder_name = folder.get("folder_name", "")
            folder_path = folder.get("folder_path", "")
            summary = folder.get("summary", "")
            services = folder.get("services_names", "")

            result += f"{indent}<folder level=\"{level}\">\n"
            result += f"{indent}  <name>{folder_name}</name>\n"
            result += f"{indent}  <path>{folder_path}</path>\n"

            if level == 0:
                # Top-level: include summary
                result += f"{indent}  <summary>{summary}</summary>\n"
            else:
                # Subfolder: include services
                result += f"{indent}  <services>{services}</services>\n"

            result += f"{indent}</folder>\n"

            if "sub_folders" in folder and isinstance(folder["sub_folders"], list):
                for subfolder in folder["sub_folders"]:
                    process_folder(subfolder, level + 1, indent)

        process_folder(folder_data)
        result += "</metadata>\n"

    return result


def format_generated_service_response(response):
    try:
        json_code_start = "```json"
        json_code_end = "```"
        start_index = response.find(json_code_start) + len(json_code_start)
        end_index = response.find(json_code_end, start_index)

        if start_index != -1 and end_index != -1:
            json_response = response[start_index:end_index].strip()
            # print("AI Response---", json_response)
            return json_response, 200
        else:
            return "", 400

    except Exception as e:
        error_logger.error(f"Error in format_generated_service_response: {e}, Traceback: {traceback.format_exc()}")
        return "", 400
    
def get_executive_summary(
    user_id,
    task_id,
    git_urls,
    repo_summary_list,
    folder_summary_list,
    is_folder_mapping=False,
    user_feedback=None,
    user_mvf:Optional[List]=[],
    model_data:Optional[dict]=None,
):
    try:
        info_logger.info(f"Generating executive summary for task ID: {task_id}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.EXECUTIVE_SUMMARY.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )
        completion_status = True
        error_message = ""
        final_combined_summary = ""
        final_combined_score = ""
        model_used = "Claude 3.5"
        max_retries = 5
        retry_delay = 2

        combined_summary = (
            format_batchwise_summary_metadata_for_services(repo_summary_list)
            if not is_folder_mapping
            else format_top_level_only(folder_summary_list)
        )
        system_prompt, user_prompt = executive_summary_generation(
        combined_summary, user_mvf
    )
        final_combined_json = None

        final_combined_json = generate_llm_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt + "User Defined Features : " + str(user_mvf),
            model_data=model_data,
            user_feedback=user_feedback
        )
        status_code = 200
        if status_code == 200:
            final_combined_json = fetch_and_parse_jsonstring_data(
                final_combined_json, max_retries, retry_delay
            )
        if final_combined_json:
            final_combined_summary = final_combined_json.get("data", "")
            final_combined_score = final_combined_json.get("confidence_score", "")
        
        info_logger.info(f"Final Summary: {final_combined_summary}")
        info_logger.info(f"Final Score: {final_combined_score} | Model Used: {model_data}")

        def remove_citations(text):
            try:
                # Remove any line that starts with "citation:"
                cleaned_text = re.sub(r'citation:.*(?:\n|$)', '', text)
                # Remove any extra newlines caused by the removal
                cleaned_text = re.sub(r'\n\s*\n', '\n\n', cleaned_text)
                cleaned_text = cleaned_text.strip()
            except:
                return text
            return cleaned_text.strip()
        final_combined_summary = remove_citations(final_combined_summary)

        update_executive_summary_in_db(
            task_id,
            final_combined_summary,
            final_combined_score,
            completion_status,
            error_message,
            model_used
        )
        
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.EXECUTIVE_SUMMARY.value,
            status=PossibleStatuses.COMPLETED.value if completion_status else PossibleStatuses.FAILED.value,
            error=error_message
        )

        return (
            final_combined_summary,
            final_combined_score,
            completion_status,
            error_message,
        )
    except Exception as e:
        error_msg = f"Error in get_executive_summary: {e} Traceback: {traceback.format_exc()}"
        error_logger.error(error_msg)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.EXECUTIVE_SUMMARY.value,
            status=PossibleStatuses.FAILED.value,
            error=error_msg
        )
        raise Exception(error_msg)




def format_file_summaries(file_summaries):
    metadata_str = "FILE-BY-FILE SUMMARIES OF REPO CONTENT\n\n"

    for file_data in file_summaries:
        git_url = file_data.get("git_url", "Unknown Git URL")
        file_path = file_data.get("file", "Unknown file path")
        summary = file_data.get("summary", "No summary available")

        metadata_str += f"-----\nGit URL:\n```{git_url}```\n\n"
        metadata_str += f"File path:\n```{file_path}```\n\n"
        metadata_str += f"Summary:\n\"\"\"\n{summary}\n\"\"\"\n\n"

    return metadata_str.strip()

def attach_rag_agent(assistantName: str, executive_summary: str, model: str):
    try:
        egpt_token = login_to_egpt()
        chatbot_id = get_chatbot_id(assistant_name=assistantName)
        info_logger.info(f"Attaching RAG Agent to Chatbot ID: {chatbot_id}")
        # info_logger.info(f"Token: {egpt_token}")
        payload = setup_rag_agent_config(chatbot_id, assistantName, executive_summary, model)
        headers = {
            "Authorization": f"Bearer {egpt_token}",
            "Content-Type": "application/json",
        }
        url = f"{config.EGPT_DOMAIN}/api/chatbot/linkAgentWithChatbot"
        try:
            
            # with open("rag_agent.json", "w") as file:
            #     json.dump(payload, file, indent=4)
            info_logger.info(f"URL: {url}")
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status() 
            return response.json() 
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}, 500

def attach_appmodai_agent(assistantName: str, task_id: str, user_id: str, executive_summary: str):
    try:
        egpt_token = login_to_egpt()
        chatbot_id = get_chatbot_id(assistant_name=assistantName)
        info_logger.info(f"Attaching AppmodAi Agent to Chatbot ID: {chatbot_id}")

        payload = setup_appmodai_agent_config(chatbot_id, assistantName, task_id, user_id, executive_summary)
        headers = {
            "Authorization": f"Bearer {egpt_token}",
            "Content-Type": "application/json",
        }
        url = f"{config.EGPT_DOMAIN}/api/chatbot/linkAgentWithChatbot"
        try:
            info_logger.info(f"URL: {url}") 
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json(), response.status_code
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}, 500
    
    

def set_agent_settings(chatbot_name: str, executive_summary: str, regenrate: bool = False, model_data=None):
    try:
        model_name = model_data['primary_model']['model_name']
        model_type = model_data['primary_model']['model_type']
        if model_name is None:
            model_name = "gpt-4o-2024-11-20"
        if model_name == "claude-3-5-haiku":
            model_name = "claude-3.5-haiku"
        if model_name == "claude-3-5-sonnet":
            model_name = "claude-3.5-sonnet"
        egpt_token = login_to_egpt()
        endpoint_get_chat_setting_id_url = f"{config.EGPT_DOMAIN}/api/chatbot/getChatbot"
        query_params = {
            "name": chatbot_name,
            "organizationName": "84lumber"
        }
        headers = {
                "authorization": f"Bearer {egpt_token}",
                "content-type": "application/json"
            }
        response = requests.get(endpoint_get_chat_setting_id_url, params=query_params, headers=headers).json()
        agent_setting_id = response.get("agentSettingsId", None)
        # info_logger.info(f"Chatbot Setting ID: {agent_setting_id}")
        if agent_setting_id is None:
            raise ValueError("Chatbot setting id not found")
        else:
            endpoint_get_agent_settings_url = f"{config.EGPT_DOMAIN}/api/agent/getAgentSettings"
            query_params = {
                "agentSettingsId": agent_setting_id
            }
            agent_settings = requests.get(endpoint_get_agent_settings_url, params=query_params, headers=headers).json()
            # info_logger.info(f"Response from getAgentSettings: {agent_settings}")
            updated_prompt = agent_settings_orchestrator_prompt.replace("{executive_summary}", executive_summary)
            # info_logger.info(f"Updated Prompt: {updated_prompt}")
            agent_settings['agentSettings']['agentSettingsId'] = agent_setting_id
            agent_settings['agentSettings']['orchestratorPrompt'] = updated_prompt
            

            if regenrate == False:
                model_type = get_model_type(model_name)
                agent_settings['agentSettings']['modelType'] = model_type
                agent_settings['agentSettings']['modelName'] = model_name
                agent_settings['agentSettings']['additionalPoints'] = agent_settings_additional_points
                agent_settings['agentSettings']['dbconfig']['host'] = config.EGPT_ALLOY_DB_HOST
                agent_settings['agentSettings']['dbconfig']['password'] = config.EGPT_ALLOY_DB_PASSWORD
                agent_settings['agentSettings']['useGuide'] = True
                agent_settings['agentSettings']['isPasswordEncrypted'] = True
                agent_settings["agentSettings"]['scopeMetrics'] = [{"id": "abcd",
                                                                    "isSelected": True,
                                                                    "order": 0,
                                                                    "scope": "Assistant",
                                                                    "similarity_threshold": 61,
                                                                    "similarity_threshold_IL_plus": 51,
                                                                    "table": "techolution-variant-concierge",
                                                                    "top_k": 10},
                                                                    {"id" : [],
                                                                    "isSelected" : False,
                                                                    "order" : 1,
                                                                    "scope" : "Custom",
                                                                    "similarity_threshold" : 0,
                                                                    "similarity_threshold_IL_plus" : 59,
                                                                    "table" : "techolution-variant-custom-filters",
                                                                    "top_k" : 0},
                                                                    { 
                                                                    "id" : "abcd",
                                                                    "isSelected" : False,
                                                                    "order" : 2,
                                                                    "scope" : "User",
                                                                    "similarity_threshold" : 0,
                                                                    "similarity_threshold_IL_plus" : 59,
                                                                    "table" : "techolution-variant-users",
                                                                    "top_k" : 0},
                                                                    { 
                                                                    "id" : "abcd",
                                                                    "isSelected" : False,
                                                                    "order" : 3,
                                                                    "scope" : "Global",
                                                                    "similarity_threshold" : 0,
                                                                    "similarity_threshold_IL_plus" : 59,
                                                                    "table" : "techolution-variant-concierge",
                                                                    "top_k" : 0}]
        

            # with open("agent_settings.json", "w") as file:
            #     json.dump(agent_settings, file, indent=4)
                
            endpoint_set_agent_settings_url = f"{config.EGPT_DOMAIN}/api/agent/saveAgentSettings"
            update_response = requests.post(endpoint_set_agent_settings_url, json=agent_settings.get("agentSettings"), headers=headers)
            update_response.raise_for_status()
            # info_logger.info(f"Agent Settings Updated: {update_response.json()} with status code: {update_response.status_code}")
            return update_response.status_code
        
    except Exception as e:
        error_logger.error(f"Error in set_agent_settings: {e} Traceback: {traceback.format_exc()}")
        raise

def update_rag_agent_settings(chatbot_name: str, executive_summary: str):
    try:
        chatbot_id = ObjectId(get_chatbot_id(assistant_name=chatbot_name))
        updated_prompt = contextaware_tool_settings_orchestrator_prompt.replace("{executive_summary}", executive_summary)
        result = egpt_agents_collection.update_one(
            {
                "utilityFunctionLink": f"{config.EGPT_DOMAIN}/utility/rag-agent/",
                "chatbotId": chatbot_id,
            },
            {
                "$set": {
                    "tools.ContextAwareResponseTool.properties.context.value": updated_prompt
                }
            }
        )
        if result.matched_count == 0:
            error_logger.warning(f"No matching document with existing field for chatbot_id {chatbot_id}")

        info_logger.info(f"Updated agent settings for chatbot '{chatbot_name}' with ID {chatbot_id}")
        
        return f"Agent settings updated successfully {updated_prompt}", 200
    except Exception as e:
        error_logger.error(f"Error updating chatbot '{chatbot_name}': {e} Traceback: {traceback.format_exc()}")
        raise

        
def get_feature_summary(user_id, task_id, file_summary_list, batch_summary_list, folder_summary_json, is_folder_mapping = False,  user_feedback=None, max_retries=3,
                        user_mvf:Optional[List]=[], executive_summary:Optional[str]= "", model_data:Optional[str]=None):
    from analyser_repo_pipeline.prompts import prepare_system_prompt

    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FEATURE_SUMMARY.value,
        status=PossibleStatuses.IN_PROGRESS.value
    )
    info_logger.info(f"Generating feature summary for task ID: {task_id}")
    completion_status = True
    error_messages = []
    retry_count = 0
    model_used = ""
    feature_summary = None
    feature_summary_score = None

    system_prompt, user_prompt = feature_summary_generation(
        formatted_data=file_summary_list,
        user_mvf=user_mvf,
        executive_summary=executive_summary
    )

    response = generate_llm_response(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model_data=model_data,
        user_feedback=user_feedback,
    )
    response = fetch_and_parse_jsonstring_data(response.strip())
    feature_summary = response.get("data", "")
    feature_summary_score = response.get("confidence_score", "")

    if not feature_summary:
        error_messages.append("Error: Unable to generate feature summary after multiple attempts.")
        log_into_bigquery("get_feature_summary", user_id, task_id, 
                          "Error: Unable to generate feature summary after multiple attempts.", 400)
        completion_status = False

    error_message = "; ".join(error_messages)

    info_logger.info(f"Feature Summary: {feature_summary}")
    info_logger.info(f"Feature Summary Score: {feature_summary_score}")
    
    update_feature_summary_in_db(
        task_id,
        feature_summary or "",
        feature_summary_score or "",
        completion_status,
        error_message,
        model_used or "",
        system_prompt,
        user_prompt
    )

    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FEATURE_SUMMARY.value,
        status=PossibleStatuses.COMPLETED.value if completion_status else PossibleStatuses.FAILED.value,
        error=error_message
    )

    return feature_summary, feature_summary_score, system_prompt



def get_feature_hierarchy(task_id, repo_json_list):
    from analyser_repo_pipeline.prompts import prepare_system_prompt


    completion_status = True
    error_message = ""
    summary_prompt = prepare_system_prompt(input_data=repo_json_list, input_type="file-by-file")
    user_prompt = feature_hierarchy_prompt()
    feature_hierarchy_response, status_code = get_ai_response_predict_api_refactoringcode(summary_prompt, user_prompt, messages=[]) #USER ID should be passed
    feature_hierarchy_data = {}
    if feature_hierarchy_response is not None and status_code == 200:
        feature_hierarchy_response = feature_hierarchy_response.strip()
        feature_hierarchy_data = fetch_and_parse_jsonstring_data(feature_hierarchy_response)
        model_used = "Claude 3.5"
    else:
        feature_hierarchy_response, status_code = gemini_call_flash_2(system_prompt=summary_prompt, user_prompt=user_prompt) #USER ID should be passed
        if feature_hierarchy_response is not None:
            feature_hierarchy_response = feature_hierarchy_response.strip()
            feature_hierarchy_data = fetch_and_parse_jsonstring_data(feature_hierarchy_response)
            model_used = "Gemini"
        else:
            model_used = ""
            feature_hierarchy_data = {}


    update_feature_hierarchyhierarchy_in_db(task_id, feature_hierarchy_data, completion_status, error_message, model_used, system_prompt, user_prompt)
    return feature_hierarchy_data

def get_executive_summary_score(summary_json):
    final_score = summary_json.get("confidence_score", "")
    return final_score



def get_repo_files(repo_url, dependancy_ref, max_retries=3, retry_delay=2):
    file_paths = []
    try:
        for attempt in range(max_retries):
            try:
                ref_data = requests.get(url=f"{config.RLEF_DOMAIN}/backend/resource/{dependancy_ref[repo_url]}").json()
                down_url = ref_data.get('resource')
                if down_url:
                    break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to get ref_data for {repo_url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise e

        for attempt in range(max_retries):
            try:
                dependancy_data = requests.get(url=down_url).json()
                req_files = dependancy_data.get('files')
                if req_files:
                    break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to get dependency data for {repo_url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise e

        for file in req_files:
            file_paths.append(file["path"])

    except Exception as e:
        print(f"Error processing repo_url {repo_url}: {e}")
        return []

    return file_paths

def fetch_and_parse_jsonstring_data(ai_response, max_retries=3, retry_delay=2):
    for attempt in range(max_retries):
        try:
            # ✅ Case 0: Already a dict — return as is
            if isinstance(ai_response, dict):
                return ai_response

            content = str(ai_response).strip()

            # ✅ Case 1: Extract from ```json ... ``` block
            fenced_match = re.search(r'```json(.*?)```', content, re.DOTALL)
            if fenced_match:
                content = fenced_match.group(1).strip()

            # ✅ Case 2: Try to parse as standard JSON
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass

            # ✅ Case 3: Try to parse as Python literal (e.g., dict-style string)
            try:
                parsed = ast.literal_eval(content)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

            # ✅ Case 4: Optional repair step (if you have a repair_json function)
            try:
                fixed = repair_json(content)
                return json.loads(fixed)
            except Exception:
                pass

            print("Retrying after failure to parse...")
            time.sleep(retry_delay)

        except Exception as e:
            print("NON PARSABLE AI RESPONSE :",ai_response)
            print(f"Error while parsing AI response: {str(e)}")

    return {}


def get_complete_v2_architecture(task_id, summary, model_data):
    completion_status = 500
    new_architecture = {}
    try:
        info_logger.info(f"Generating complete architecture for task ID: {task_id}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.ARCHITECTURE_DIAGRAM.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )
        icons_metadata = get_icons_metadata()
        # Call the external service to generate the new architecture
        system_prompt, user_prompt = architecture_diagram_generation(services_data=summary, icons_metadata=icons_metadata)
        new_architecture = generate_llm_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model_data=model_data,
        )
        new_architecture = fetch_and_parse_jsonstring_data(new_architecture)
        error = ""
        completion_status = 200 if new_architecture else 500
        # Save the new architecture into the database
        info_logger.info(f"New Architecture: {new_architecture}")
        update_v2_archcompletion_status_in_db(task_id, new_architecture,completion_status, error)
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.ARCHITECTURE_DIAGRAM.value,
            status=PossibleStatuses.COMPLETED.value if completion_status == 200 else PossibleStatuses.FAILED.value,
            error=error
        )
        return new_architecture

    except Exception as e:
        print(f"Error in get_complete_v2_architecture: {e} \n\n Traceback: {traceback.format_exc()}")
        error = f"Error in get_complete_v2_architecture: {e} \n\n Traceback: {traceback.format_exc()}"
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.ARCHITECTURE_DIAGRAM.value,
            status=PossibleStatuses.FAILED.value,
            error=error
        )
        update_v2_archcompletion_status_in_db(task_id, new_architecture, completion_status, error)
    # Return None if any exception occurs
    return None

def count_tokens(data):
    try:
        model = GenerativeModel("gemini-2.0-flash") #Changed to flash currently as this 1.5-pro gemini was throwing an error for deployment not found
        response = model.count_tokens(data)
        print(f"Prompt Token Count: {response.total_tokens}")
        print(f"Prompt Character Count: {response.total_billable_characters}")
        return response.total_tokens
    except Exception as e:
        error_logger.error(f"Error in count_tokens: {e} \n\n Traceback: {traceback.format_exc()}")
        return config.GEMINI_TOKEN_THRESHOLD + 1




def fix_github_repo_url(repo_url: str, use_ssh: bool = False) -> str:
    """
    Fix GitHub repository URL to ensure it's in a cloneable format.
    
    :param repo_url: The GitHub repository URL to fix.
    :param use_ssh: If True, return SSH URL, otherwise return HTTPS URL.
    :return: Fixed GitHub repository URL.
    """
    match = re.match(r'https?://github\.com/([^/]+/[^/]+)', repo_url)
    
    if not match:
        raise ValueError(f"Invalid GitHub URL: {repo_url}")
    
    repo_path = match.group(1)
    
    if use_ssh:
        return f"git@github.com:{repo_path}.git"
    else:
        return f"https://github.com/{repo_path}.git"
    
    
def store_file_count_for_repositories(task_id: str, user_id: str, local_repo_paths: list): 
    info_logger.info(f"Getting file count for task ID: {task_id}")
    for local_repo_path in local_repo_paths:
        try:
            os.chdir(local_repo_path)
            cmd_output = subprocess.check_output("git ls-files | wc -l", shell=True)
            info_logger.info(
                f"Number of files in the repository: {cmd_output.decode('utf-8')}"
            )
            total_files = int(cmd_output.decode("utf-8"))
            project_summary_db.projects.update_one(
                {"task_id": task_id},
                {"$addToSet": {"total_file_count": total_files}},
            )
        except Exception as e:
            error_logger.error(
                f"Error: Unable to get the file count for the repository: {local_repo_path}"
                + " "
                + str(e)
                + "```"
            )
            log_into_bigquery(
                "project_analyzer_process_v2",
                user_id,
                task_id,
                f"Error: Unable to get the file count for the repository: {local_repo_path}"
                + " "
                + str(e)
                + "```",
            )
            
            
def chunk_list(data, chunk_size=5):
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def process_feature_hierarchy(file_summary_list, user_id, task_id, mvfs, feature_summary:Optional[str]="", model_data: Optional[dict]=None, embedding_model_data: Optional[dict]=None):
    import analyser_repo_pipeline.feature_hierarchy_generation_pipeline.FeatureHierarchy as fh
    completion_status = True
    output_file = f"feature_embeddings_{task_id}_{uuid.uuid4()}.npz"

    try:
        info_logger.info(f"Generating feature hierarchy for task ID: {task_id}")
        # info_logger.info(f"G: {file_summary_list}")

        # summary_list_file = f"file_summary_list_{task_id}.json"
        # with open(summary_list_file, "w") as f:
        #     json.dump(file_summary_list, f, indent=4)
        
        # info_logger.info(f"Saved file_summary_list to {summary_list_file}")
        
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FEATURE_HIERARCHY.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        fh_instance = fh.FeatureHierarchy(file_summary_list, task_id=task_id, model_data=model_data, embedding_model=embedding_model_data['primary_model']['model_name'])
        feature_hierarchy_data = fh_instance.generate_summary()

        # with open("stage_0_feature_hierarchy.json", "w") as f:
        #     json.dump(feature_hierarchy_data, f, indent=4)
        
        info_logger.info(f"Length of feature_hierarchy_data before chunking: {len(feature_hierarchy_data)}")
        info_logger.info(f"Datatype of feature_hierarchy_data: {type(feature_hierarchy_data)}")
        info_logger.info(f"Length of feature_hierarchy_data after chunking: {len(feature_hierarchy_data)}")
        embeddings_data = process_features(feature_hierarchy_data, embedding_model_data['primary_model']['model_name'])
        # Save the embeddings
        save_embeddings(embeddings_data, output_file)
        
        
        stage_1_feature_hierarchy = merge_features(original_data=feature_hierarchy_data, embeddings_file=output_file, model_data = model_data)
        # with open("stage_1_feature_hierarchy.json", "w") as f:
        #     json.dump(stage_1_feature_hierarchy, f, indent=4)

        clustering_name_json = reduce_number_of_features(stage_1_feature_hierarchy, embedding_model_data['primary_model']['model_name']) 
        
        final_names_list = mapping_new_names_after_clustering(cluster_result=clustering_name_json, stage_1_feature_hierarchy=stage_1_feature_hierarchy, model_data=model_data)
        
        
        stage_2_feature_hierarchy = generate_final_feature_hierarchy(final_names_json=final_names_list, stage_1_feature_hierarchy=stage_1_feature_hierarchy)
        
        # with open("stage_2_feature_hierarchy.json", "w") as f:
        #     json.dump(stage_2_feature_hierarchy, f, indent=4)
        
        
        stage_3_feature_hierarchy = reduce_number_of_sub_features(stage_2_feature_hierarchy, model_data, embedding_model_data['primary_model']['model_name'])

        # with open("stage_4_MVFS_feature_hierarchy.json", "w") as f:
        #     json.dump(stage_4_MVFS_feature_hierarchy, f, indent=4)

        # feature_list_file = f"feature_hieratchy_list{task_id}.json"
        # with open(feature_list_file, "w") as f:
        #     json.dump(stage_3_feature_hierarchy, f, indent=4)
        
        # info_logger.info(f"Saved file_summary_list to {summary_list_file}")

        # info_logger.info(f"Length of stage_4_MVFS_feature_hierarchy: {len(stage_4_MVFS_feature_hierarchy)}")
        # info_logger.info(f"Datatype of stage_4_MVFS_feature_hierarchy: {type(stage_4_MVFS_feature_hierarchy)}")
        # info_logger.info(f"Length of stage_4_MVFS_feature_hierarchy after chunking: {stage_4_MVFS_feature_hierarchy}")
        
        # with open("stage_3_feature_hierarchy.json", "w") as f:
        #     json.dump(stage_3_feature_hierarchy, f, indent=4)
        
        feature_hierarchy_data = post_processing(stage_3_feature_hierarchy, model_data = model_data)
        # Add Misc Files with Ft. Hierarchy
        missed_files = fh_instance.absent_paths
        info_logger.info(f"Missed files: {missed_files}")
        
        
        if len(missed_files) > 0:
            ## Re clustering the uncategorized files
            missed_file_summary = [
                i
                for i in file_summary_list
                if i.get("file", "") in missed_files
            ]  ## File Summary for missed files by AI
            
            ft_recluster = fh.FeatureHierarchy(missed_file_summary, task_id=task_id, model_data=model_data, embedding_model=embedding_model_data['primary_model']['model_name'])
            re_cluster_ft = ft_recluster.generate_summary()

            # print("Re cluster Features", re_cluster_ft)
            feature_hierarchy_data.extend(
                re_cluster_ft
            )  ## Adding the re clustering result in the same list
            missed_files = ft_recluster.absent_paths
        else:
            missed_files = []

        feature_hierarchy_data = post_processing(
            feature_hierarchy_data, model_data
        )  
        
        misc_data = add_misc_files_to_ft_hierarchy(
            missed_files, file_summary_list
        )
        fh_data = {
            "features": feature_hierarchy_data,
            "miscellaneous": misc_data,
        }
        
        final_feature_hierarchy = normalize_confidence_scores(fh_data)
        # adding priority to medium if missed
        [feature.update({"priority": "medium"}) for feature in final_feature_hierarchy['features'] if not feature.get("priority", "")]

        # with open("final_feature_hierarchy_data.json", "w") as f:
        #     json.dump(final_feature_hierarchy, f, indent=4) 
        try:
            if mvfs or feature_summary:
                info_logger.info(f"Using MVFS for feature hierarchy generation.")
                final_feature_hierarchy = merging_similar_features(final_feature_hierarchy, mvfs, feature_summary, model_data=model_data)
            else:
                info_logger.info(f"No user defined MVFs found for feature hierarchy generation.")
        except Exception as e:
            traceback.print_exc()
            print("merging features failed due to -> ", str(e))
        

        try:
            final_feature_hierarchy = reorder_feature_hierarchy(final_feature_hierarchy, feature_summary, mvfs, embedding_model=embedding_model_data['primary_model']['model_name'])
        except Exception as e:
            print("Reordering failed due to -> ", str(e))

        with open("final_feature_hierarchy.json",'w') as f:
                json.dump(final_feature_hierarchy, f , indent = 4)
        update_feature_hierarchyhierarchy_in_db(
            user_id,
            task_id,
            final_feature_hierarchy,
            completion_status,
            "error_message",
            "Claude 3.5",
            "system_prompt",
            "user_prompt",
        )
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FEATURE_HIERARCHY.value,
            status=PossibleStatuses.COMPLETED.value,
        )
            
    
    except Exception as e:
        error = f"Error occurred on Feature Hierarchy generation: {str(e)} {traceback.format_exc()}"
        error_logger.error(error)
        default_feature_hierarchy = {
            "features": [],
            "miscellaneous": [],
        }
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FEATURE_HIERARCHY.value,
            status=PossibleStatuses.FAILED.value,
            error=error
        )
        update_feature_hierarchyhierarchy_in_db(
            user_id,
            task_id,
            default_feature_hierarchy, 
            False,
            error,
            "Claude 3.5",
            "system_prompt",
            "user_prompt",
        )
    
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)
            
def process_folder_summary_generation(github_urls, local_repo_paths, user_id, task_id, file_summary_list, model_data:Optional[dict]=None):
    update_step_status(
        task_id=task_id,
        step_name=StepStatus.FOLDER_SUMMARY.value,
        status=PossibleStatuses.IN_PROGRESS.value,
    )
    try:
        info_logger.info(f"Generating folder summary for task ID: {task_id}")
        folder_summaries_generated = []

        def generate_summary(index, local_repo_path):
            try:
                folder_summary = generate_folder_summaries_pipeline(
                    repo_root_path=local_repo_path,
                    file_summaries=file_summary_list,
                    task_id=task_id,
                    model_data = model_data
                )
                final_data = {
                    "git_url": github_urls[index],
                    "folder_data": folder_summary
                }
                info_logger.info(f"Folder Summary generated for url {github_urls[index]}")
                return final_data
            except Exception as e:
                error_logger.error(f"Error generating summary for {github_urls[index]}: {str(e)} Traceback: {traceback.format_exc()}")
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.FOLDER_SUMMARY.value,
                    status=PossibleStatuses.WARNING.value,
                    error=f"Error generating summary for {github_urls[index]}: {traceback.format_exc()}"
                )
                return { "git_url": github_urls[index], "folder_data": {} }

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(generate_summary, idx, path): idx
                for idx, path in enumerate(local_repo_paths)
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    folder_summaries_generated.append(result)

        
        # Create a ThreadPoolExecutor for background tasks
        def background_tasks(folder_summaries):
            try:
                update_folder_summaries_in_db(user_id, task_id, folder_summaries)
                save_to_ecg_db(folder_summaries, task_id)
                generate_design_pattern(task_id=task_id, data=folder_summaries)
            except Exception as e:
                error_logger.error(f"Error in background tasks: {str(e)}")
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.FOLDER_SUMMARY.value,
                    status=PossibleStatuses.WARNING.value,
                    error=f"Error in background tasks: {str(e)}, {traceback.format_exc()}"
                )
                raise e

        # Start background tasks in a separate thread
        ThreadPoolExecutor().submit(background_tasks, folder_summaries_generated)
        
        info_logger.info(f"Folder summaries generated successfully for task ID: {task_id}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FOLDER_SUMMARY.value,
            status=PossibleStatuses.COMPLETED.value
        )

        return folder_summaries_generated
    except Exception as e:
        error_logger.error(f"Error occurred on folder summary generation: {str(e)} {traceback.format_exc()}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FOLDER_SUMMARY.value,
            status=PossibleStatuses.FAILED.value,
            error=f"Error occurred on folder summary generation: {str(e)} {traceback.format_exc()}"
        )
        return []
                                                                
def enable_folder_mapping(task_id, user_id, git_pat_token):
    from ingest_project_summary import get_component_data_from_db
    intial_dir = os.getcwd()
    try:
        info_logger.info(f"Enabling folder mapping for task ID: {task_id}")
        
        repo_folder = initialize_project(task_id)
        projects_data = project_summary_db.projects.find_one({"task_id": task_id})
        
        if not projects_data:
            error_logger.error(f"No project data found for task ID: {task_id}")
            return False, "Project data not found"
        
        github_urls, branch_names, rlef_ids, file_summary_list, repo_summary_list, folder_summary_json, mvfs, is_folder_mapping, executive_summary, settings_config = (
        get_component_data_from_db(user_id, task_id)
        )                                 
        
        # with open("file_summary_testingggg.json", "w") as f:
        #     json.dump(file_summary_list, f, indent=4)      
            
        # with open("batch_summary_testingggg.json", "w") as f:
        #     json.dump(repo_summary_list, f, indent=4)                
        
        clone_threads = clone_repositories(
            github_urls, branch_names, git_pat_token, repo_folder, user_id, task_id
        )
        local_repo_paths = [thread.join() for thread in clone_threads]
        
        
        
        for item in file_summary_list:
            item["file"] = item.pop("file_path")
            item["summary"] = item.pop("file_summary")
            if "services_name" not in item:
                item["service_names"] = ""


        folder_summaries_generated = process_folder_summary_generation(github_urls,local_repo_paths, user_id, task_id, file_summary_list, settings_config['category_config']['project_analyzer']['model_config']['folder_summary'])
        
        if not folder_summaries_generated:
            error_logger.error(f"Error in enabling folder mapping: {folder_summaries_generated}")
            return False, "Error in enabling folder mapping"
        
        
        project_summary_db.projects.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "is_folder_mapping": True,
                },
                
            },
            upsert=True,
        )
        
        return True, "Folder mapping enabled successfully"
        
    except Exception as e:
        error_logger.error(f"Error occurred on folder mapping enabling: {str(e)} {traceback.format_exc()}")
        return False, str(e)
    finally:
        os.chdir(intial_dir)
        remove_repo_folder(repo_folder)
        
    