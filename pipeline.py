import json
import os
import shutil
import subprocess
import threading
import traceback
import logging
from analyser_repo_pipeline.feature_hierarchy_generation_pipeline.FHPostProcessing import (
    add_misc_files_to_ft_hierarchy,
    post_processing,
)
from analyser_repo_pipeline.egpt_helpers import extract_and_ingest_files
from analyser_repo_pipeline.pipeline_helpers import (
    ThreadWithReturnValue,
    clone_repositories,
    fetch_repository_summaries,
    generate_dependencies,
    generate_services,
    get_complete_v2_architecture,
    get_executive_summary,
    get_feature_summary,
    get_file_paths,
    get_repository_languages,
    handle_mongo_upload,
    handle_repo_commit_hashes,
    initialize_project,
    perform_vulnerability_checks,
    process_feature_hierarchy,
    process_folder_summary_generation,
    set_agent_settings,
    store_file_count_for_repositories,
    upload_dependencies,
    upload_repositories,
    attach_rag_agent,
    attach_appmodai_agent,
)
from analyser_repo_pipeline.rlef_helpers import remove_keys_recursively
from analyser_repo_pipeline.payload_schema import MVF, WTA
from clone_repo import get_current_commit_hash
import config
from crashanalytics import log_into_bigquery
from db_update import (
    tracking_error_in_pipeline,
    update_batch_summaries_in_db,
    update_failed_executive_summary_status_in_db,
    update_feature_hierarchyhierarchy_in_db,
    update_file_paths_in_db,
)
from email_utils import send_completion_mail, send_pipeline_error_email
from generalised_utils import remove_repo_folder
from helper_func import generate_combined_json_strings
from ingest_project_summary import fetch_email_status
from models import project_summary_db
from access_control_pipeline.db_operations import increment_analyze_count
from logger import info_logger, error_logger, warning_logger
from typing import List, Optional

from analysis_status_update import update_task_fields, update_step_status
from utils.analysis_status_enums import StepStatus, PossibleStatuses

def project_analyzer_process_v2(
    user_id,
    task_id,
    github_urls,
    branch_names,
    git_pat_token,
    egpt_token,
    assistant_id,
    folder_name,
    batch_size,
    project_name,
    chatbot_name,
    github_info,
    active_user_id: str,
    regen: bool = False,
    ingestion_model_type=None,
    ingestion_model_name=None,
    chat_model_type=None,
    chat_model_name=None,
    mvfs: Optional[List] = [],  # name desc WTA \WNTA
):
    intial_dir = os.getcwd()
    skip_remaining_steps = False

    update_task_fields(
        task_id=task_id,
        overall_status=PossibleStatuses.IN_PROGRESS.value,
        overall_coverage=0
    )
    try:
        from analyser_repo_pipeline.api_helpers import get_setting_config
        from server import mongo_reddis_config
        info_logger.info(
            f"Starting the project analyzer process for task_id: {task_id}"
        )
        # Step 1: Initialize the project by setting up the repo folder and cleaning any previous folders

        info_logger.info("Setting up the project directory")
        repo_folder = initialize_project(task_id)

        #Step 1.1: Fetching settings from MongoDB or Redis Cache
        settings_config = get_setting_config(mongo_reddis_config, task_id)
        project_analyzer_config = settings_config['category_config']['project_analyzer']
        print(f"SETTINGS for {task_id}:\n{project_analyzer_config}" )
        # Step 2: Clone GitHub repositories using threading
        clone_threads = clone_repositories(
            github_urls, branch_names, git_pat_token, repo_folder, user_id, task_id
        )
        local_repo_paths = [thread.join() for thread in clone_threads]

        commit_hashes = handle_repo_commit_hashes(
            local_repo_paths=local_repo_paths,
            user_id=user_id,
            task_id=task_id,
            github_info_list=github_info,
        )

        # Step 2.5: Get the file COUNTS for each repository
        store_file_count_for_repositories(task_id, user_id, local_repo_paths)
        os.chdir(intial_dir)
        
        # Step 3: Get repository languages for each GitHub repository using threading
        repo_language_list = get_repository_languages(
            task_id, user_id, github_urls, git_pat_token
        )

        # Step 4: Upload the repositories to EGPT using threading
        upload_threads = upload_repositories(
            task_id,
            github_urls,
            git_pat_token,
            branch_names,
            commit_hashes,
            egpt_token,
            assistant_id,
            batch_size,
            project_analyzer_config['model_config']['file_summary']['primary_model']['model_type'],
            project_analyzer_config['model_config']['file_summary']['primary_model']['model_name'],
        )
        [thread.join() for thread in upload_threads]

        # Step 5: Perform MongoDB uploads for tracking RLEF IDs
        rlef_ids = handle_mongo_upload(
            user_id, task_id, chatbot_name, project_name, github_urls
        )
        # with open("appmod_rlef_ids.json", "w") as f:
        #     json.dump(rlef_ids, f, indent=4)

        # Step 6: Upload files to EGPT using threading if provided
        file_upload_threads = []
        if os.path.exists(folder_name):
            file_upload_thread = ThreadWithReturnValue(
                target=extract_and_ingest_files,
                args=(folder_name, task_id, user_id, assistant_id),
            )
            file_upload_threads.append(file_upload_thread)
            file_upload_thread.start()

        # Step 7: Perform vulnerability checks on each local repository path using threading
        vulnerability_threads = perform_vulnerability_checks(
            user_id, task_id, local_repo_paths, github_urls
        )

        def monitor_vulnerability_checks():
            vulnerability_checks = [thread.join() for thread in vulnerability_threads]

        monitor_thread = threading.Thread(target=monitor_vulnerability_checks)
        monitor_thread.start()

        # Step 8: Fetch summaries for each repository from RLEF IDs using threading
        batch_summary_list, file_summary_list, rlef_data_list = (
            fetch_repository_summaries(user_id, task_id, rlef_ids, chatbot_name=chatbot_name)
        )
        # print("file_summary_list : ",file_summary_list)
        if not batch_summary_list or not file_summary_list or not rlef_data_list:
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
        # info_logger.info(f"Final Batch Summary List : {batch_summary_list}")
        # info_logger.info(f"Final File Summary List : {file_summary_list}")
        # info_logger.info(f"Final RLEF Data List : {rlef_data_list}")
        update_batch_summaries_in_db(
            user_id, task_id, batch_summary_list, file_summary_list, rlef_data_list
        )

        # with open("batch_summary_list.json", "w") as f:
        #     json.dump(batch_summary_list, f, indent=4)
          
        # with open("file_summary_list.json", "w") as f:
        #     json.dump(file_summary_list, f, indent=4)
          
        # with open("rlef_data_list.json", "w") as f:
        #     json.dump(rlef_data_list, f, indent=4)

        # Step 9: Creating Dependency Graph and Folder Summary in Parallel
        dependency_thread = ThreadWithReturnValue(
            target=generate_dependencies,
            kwargs={
                "task_id": task_id,
                "git_pat_token": git_pat_token,
                "repo_urls": github_urls,
                "local_repo_paths": local_repo_paths,
                "languages_lists": repo_language_list,
                "file_by_file_summaries": file_summary_list,
            },
        )
        print("FOLDER SUMMARY GEN STARTED..")
        folder_summary_thread = ThreadWithReturnValue(
            target=process_folder_summary_generation,
            args=(github_urls,local_repo_paths, user_id, task_id, file_summary_list, project_analyzer_config['model_config']['folder_summary']),
        )

        dependency_thread.start()
        folder_summary_thread.start()

        depenency_ref, dependency_graph, status_code = dependency_thread.join()
        folder_summary_json = folder_summary_thread.join()

        # with open("process_folder_summary_generation.json",'w') as f:
        #     json.dump(folder_summary_json, f , indent = 4)

        try:
            dependency_graph = remove_keys_recursively(dependency_graph, ["content"])
        except Exception as e:
            error_logger.error("Error: " + str(e))
            error_logger.error(f"Dependency response : {dependency_graph}")
            log_into_bigquery("upload_dependencies", user_id, task_id, str(e))

        print("GENERATE SERVICES STARTED..")
        # Step 10: Generate Services
        combined_services_json = generate_services(
            user_id, task_id, dependency_graph, file_summary_list,status_code, folder_summary_json,True, model_data=project_analyzer_config['model_config']['generate_services']
        )
        # with open("generate_services.json",'w') as f:
        #     json.dump(combined_services_json, f , indent = 4)

        print("EXECUTIVE SUMMARY GEN STARTED..")
        # Step 11: Get executive summary and executive summary score
        (
            executive_summary,
            executive_summary_score,
            executive_summary_status,
            executive_summary_error_messages,
        ) = get_executive_summary(user_id=user_id, task_id=task_id, git_urls=github_urls, repo_summary_list=batch_summary_list, folder_summary_list = folder_summary_json, is_folder_mapping=True, user_feedback=None, user_mvf=mvfs, model_data=project_analyzer_config['model_config']['executive_summary'])
        print("executive_summary :",executive_summary)
        if not executive_summary_status:
            print("Error: Unable to get executive summary")
            print(f"Error Messages: {executive_summary_error_messages}")
            update_failed_executive_summary_status_in_db(
                task_id, executive_summary_error_messages
            )
            skip_remaining_steps = True
        
        # with open("get_executive_summary.json",'w') as f:
        #     json.dump(executive_summary, f , indent = 4)

        print("FEATURE SUMMARY GEN STARTED..")
        ## Step 11.5: Attaching agent to assistant
        rag_agent_attach_repsonse = attach_rag_agent(
            assistantName=chatbot_name,
            executive_summary=executive_summary,
            model=project_analyzer_config['model_config']['chat_inference']['primary_model']['model_name'],
        )
        # info_logger.info(f"RAG Agent Attach Response: {rag_agent_attach_repsonse}")

        appmodai_agent_attach_respones = attach_appmodai_agent(
            assistantName=chatbot_name,
            task_id=task_id,
            user_id=user_id,
            executive_summary=executive_summary,
        )
        # info_logger.info(
        #     f"AppmodAi Agent Attach Response: {appmodai_agent_attach_respones}"
        # )

        ## Step 11.6: Update agent settings
        set_agent_settings(
            chatbot_name=chatbot_name,
            executive_summary=executive_summary,
            model_data=project_analyzer_config['model_config']['chat_inference'],
        )

        if not skip_remaining_steps:
            # Running the step 12, 13, 14, 15 only if the executive summary is generated successfully
            combined_json_strings = generate_combined_json_strings(
                combined_services_json
            )
            # Step 12: Generate the architecture based on the combined summary and services

            final_architecture = get_complete_v2_architecture(
                task_id,
                executive_summary
                + f"<services> {str(combined_json_strings)} </services> ",
                model_data = project_analyzer_config['model_config']["architecture_diagram"]
            )
            # with open("get_complete_v2_architecture.json",'w') as f:
            #     json.dump(final_architecture, f , indent = 4)

            print("FEATURE HIERARCHY GEN STARTED..")
            # Step 13: Get feature summary and feature summary score
            feature_summary, feature_summary_score, summary_prompt_used = (
                get_feature_summary(
                    user_id, task_id, file_summary_list, batch_summary_list, folder_summary_json, True, user_mvf=mvfs, executive_summary=executive_summary,model_data=project_analyzer_config['model_config']['feature_summary']
                )
            )

            # with open("get_feature_summary.json",'w') as f:
            #     json.dump(feature_summary, f , indent = 4)

           # Step 14: Get feature hierarchy using output from above
            process_feature_hierarchy(
                file_summary_list, user_id, task_id, mvfs,
                feature_summary=feature_summary,  # pass it in
                model_data = project_analyzer_config['model_config']['feature_hierarchy'],
                embedding_model_data=project_analyzer_config['model_config']['embedding_model']
            )

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EMAIL_SENDING.value,
                status=PossibleStatuses.IN_PROGRESS.value,
            )
            # step 15: Checking the email status and sending the email
            email_response = fetch_email_status(user_id, task_id)
            email_status = email_response.get("email_status")
            email_id = email_response.get("email_id")
            analysis_type = email_response.get("analysis_type")

            if email_status and email_id and analysis_type:
                info_logger.info(
                    f"Email status is True and Email ID is present for user_id: {user_id}, task_id: {task_id} and analysis_type: {analysis_type}"
                )
                send_completion_mail(task_id, user_id, email_id, analysis_type)
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.EMAIL_SENDING.value,
                    status=PossibleStatuses.COMPLETED.value
                )
            else:
                if not email_status:
                    if email_status is None:
                        info_logger.info(
                            f"Email status is missing for user_id: {user_id}, task_id: {task_id}"
                        )
                        update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.EMAIL_SENDING.value,
                            status=PossibleStatuses.WARNING.value,
                            info=f"Email status is missing for user_id: {user_id}, task_id: {task_id}"
                        )
                    else:
                        info_logger.info(
                            f"Email status is False for user_id: {user_id}, task_id: {task_id}"
                        )
                        update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.EMAIL_SENDING.value,
                            status=PossibleStatuses.WARNING.value,
                            info=f"Email status is False for user_id: {user_id}, task_id: {task_id}"
                        )

                if not email_id:
                    info_logger.info(
                        f"Email ID is missing for user_id: {user_id}, task_id: {task_id}"
                    )
                    update_step_status(
                        task_id=task_id,
                        step_name=StepStatus.EMAIL_SENDING.value,
                        status=PossibleStatuses.WARNING.value,
                        info=f"Email ID is missing for user_id: {user_id}, task_id: {task_id}"
                    )


            # Step 16 Updating users_analysis_stats
            incremented: bool = True
            if regen:
                incremented = increment_analyze_count(
                    user_id=active_user_id, field="attempted_reanalyze_count"
                )
            else:
                incremented = increment_analyze_count(
                    user_id=active_user_id, field="attempted_analyze_count"
                )
            if not incremented:
                error_logger.error("Error while updating attempted counts in db")
        update_task_fields(
            task_id=task_id,
            overall_status=PossibleStatuses.COMPLETED.value
        )

    except Exception as e:
        version = config.ENV
        error = f"Error occurred in Project Analyzer process: {str(e)} \n\n Traceback: {traceback.format_exc()}"
        error_logger.error(
            error
        )
        log_into_bigquery(
            "project_analyzer_process_v2",
            user_id,
            task_id,
            f"Error occurred in Project Analyzer process: {str(e)}",
        )
        tracking_error_in_pipeline(task_id, error)

        update_task_fields(
            task_id=task_id,
            overall_status=PossibleStatuses.FAILED.value,
            overall_error_log=error,
        )

        payload_recieved = {
            "task_id": task_id,
            "user_id": user_id,
            "github_info": github_info,
            "batch_size": batch_size,
        }
        if regen:
            analysis_type = "reanalyze"
        else:
            analysis_type = "new_analysis"
        send_pipeline_error_email(
            task_id, error, version, analysis_type, payload_recieved
        )
        log_into_bigquery("get_executive_summary", user_id, task_id, error, 500)

        raise e
    finally:
        # Step 17: Clean up the repository folder after the process is complete
        os.chdir(intial_dir)
        remove_repo_folder(repo_folder)
        if os.path.exists(folder_name):
            shutil.rmtree(folder_name)
