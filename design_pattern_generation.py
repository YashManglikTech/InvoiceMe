import traceback

from predict import gemini_call_flash_2
import json
import requests
from pymongo import MongoClient
import config
from models import ecg_db

# from usertodocument.token_counter import count_tokens
# from google import genai
design_pattern_tool = {
  "name": "get_design_pattern",
  "description": "Creates design pattern of the project with the attributed of frontend, backend, database, and devops",
  "parameters": {
    "type": "object",
    "properties": {
      "design_pattern": {
        "type": "object",
        "properties": {
          "Frontend": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "name": {"type": "string", "description": "Name of the design pattern"},
                "description": {"type": "string", "description": "Description of the design pattern"},
                "example": {"type": "string", "description": "Example of the design pattern"},
                "whenToApply": {"type": "string", "description": "When to apply this design pattern"},
                "whenNotToApply": {"type": "string", "description": "When not to apply this design pattern"}
              }
            }
          },
          "Backend": {
            "type": "array",
            "items": {
              "type": "object", 
              "properties": {
                "name": {"type": "string", "description": "Name of the design pattern"},
                "description": {"type": "string", "description": "Description of the design pattern"},
                "example": {"type": "string", "description": "Example of the design pattern"},
                "whenToApply": {"type": "string", "description": "When to apply this design pattern"},
                "whenNotToApply": {"type": "string", "description": "When not to apply this design pattern"}
              }
            }
          },
          "Database": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "name": {"type": "string", "description": "Name of the design pattern"},
                "description": {"type": "string", "description": "Description of the design pattern"},
                "example": {"type": "string", "description": "Example of the design pattern"},
                "whenToApply": {"type": "string", "description": "When to apply this design pattern"},
                "whenNotToApply": {"type": "string", "description": "When not to apply this design pattern"}
              }
            }
          },
          "DevOps": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "name": {"type": "string", "description": "Name of the design pattern"},
                "description": {"type": "string", "description": "Description of the design pattern"},
                "example": {"type": "string", "description": "Example of the design pattern"},
                "whenToApply": {"type": "string", "description": "When to apply this design pattern"},
                "whenNotToApply": {"type": "string", "description": "When not to apply this design pattern"}
              }
            }
          }
        }
      }
    },
    "required": ["design_pattern"]
  }
}

# def token_counter(prompt):
#     """
#     This function takes a prompt as input and returns the number of tokens in the prompt.
#     """

#     client = genai.Client()

#     # Count tokens using the new client method.
#     return client.models.count_tokens(
#         model="gemini-2.0-flash", contents=prompt
#     )

design_pattern_system_prompt = """
You are a smart helpful agentic AI assistant capable of analyzing a codebase summary and come up with an accurate design pattern(language agnostic template or blueprint) for that project. You are mimicking an experienced software developer capable of design system level architechture for enterprise grade products.

** User Input **
You will be provided a folder/ directory level brief summarise of a code repository.

<instruction>
- You need to come up with a design pattern for the project.
- The design pattern should be in a JSON format.
- The design pattern should be divided into four sections: Frontend, Backend, Database, and DevOps.
- Each section should contain a list of design patterns.
- Each design pattern should have the following attributes:
  - name: Name of the design pattern
  - description: Description of the design pattern
  - example: Example of the design pattern
  - whenToApply: When to apply this design pattern
  - whenNotToApply: When not to apply this design pattern
- IMPORTANT DON'T assume anything which is not provided by the user, in that case keep the field as `[]`. Each repo might not have all four attributes so return `[]` if no data found. Example if the repo is related to the frontend then backend, database, devops will become `[]`, similarly if repo is related to backend then only generate design pattern for `Backend` and other three will be `[]`.
</instruction>
<response_format>
```json
"design_pattern": {
    "Frontend": [
    {
        "description": "A detail description (2-3 sentence) of the design pattern",
        "example": "A descriptive example of the design pattern",
        "name": "Name of the design pattern",
        "whenNotToApply": "Considering a very new developer when this design pattern is applicable. This needs to be in the context of the project and code base",
        "whenToApply": "When this design pattern is not applicable"
      }
    ],
    "Backend": [
      {
        "description": "A detail description (2-3 sentence) of the design pattern",
        "example": "A descriptive example of the design pattern",
        "name": "Name of the design pattern",
        "whenNotToApply": "Considering a very new developer when this design pattern is applicable. This needs to be in the context of the project and code base",
        "whenToApply": "When this design pattern is not applicable"
      }
    ],
    "Database": [],
    "DevOps": [{
        "description": "A detail description (2-3 sentence) of the design pattern",
        "example": "A descriptive example of the design pattern",
        "name": "Name of the design pattern",
        "whenNotToApply": "Considering a very new developer when this design pattern is applicable. This needs to be in the context of the project and code base",
        "whenToApply": "When this design pattern is not applicable"
      }]
  }
}```
</response_format>
"""

design_pattern_user_prompt = """Based on the following project summary, please provide a design pattern for the project. The design pattern should be in a JSON format and should be divided into four sections: Frontend, Backend, Database, and DevOps. Each section should contain a list of design patterns with the specified attributes.
<folder_summary>
{folder_summary}
</folder_summary>
"""

def get_file_summary(task_id:str, file_path:str, repo_url:str):
    """
    This function takes a task id, file path and repo url as input and returns the file summary.
    """
    # Call the API
    try:
      url = f"{config.APPMOD_DOMAIN}/api/github/file_path_imports"
      payload = json.dumps({
        "taskid": task_id,
        "file_path": file_path,
        "repo_url": repo_url,
        "content": False,
        "summary": True
      })
      headers = {
        'Content-Type': 'application/json'
      }

      response = requests.request("POST", url, headers=headers, data=payload)
      
      try:
         final_response = response.json()['files'][0]['summary']
      except:
         final_response = ""
      return final_response, 200
    except Exception as e:
      traceback.print_exc()
      print(f"Error in getting file summary for filepath: {file_path}, error_msg: {e}")
      return None, 500


def get_design_pattern(folder_summary:list[dict]):
    """
    This function takes a folder summary as input and returns a design pattern for the project.
    The design pattern is divided into four sections: Frontend, Backend, Database, and DevOps.
    Each section contains a list of design patterns with the specified attributes.
    """
    # Prepare the user prompt
    user_prompt = design_pattern_user_prompt.format(folder_summary=folder_summary)

    # Call the Gemini API
    try:
      response, status = gemini_call_flash_2(
          system_prompt=design_pattern_system_prompt,
          user_prompt=user_prompt,
          # tools=[design_pattern_tool]
      )
      if status==200:
        # Extract JSON string between curly braces
        json_str = response[response.find('{'):response.rfind('}')+1]
        # Parse JSON string to Python dict
        return json.loads(json_str)
      else:
        raise ValueError("error generating design pattern")
    except Exception as e:
      print(f"Error in generating design pattern: {e}")
      return None

def process_folder_summary(folder_data):
  """
  Recursively process folder summary data and generate design patterns.
  Args:
    folder_data (dict): Dictionary containing folder information
  Returns:
    tuple: (design pattern, token count)
  """
  folder_summary = []
  folder_summary.append({"folder_name": folder_data['folder_name'], "summary": folder_data['summary']})
  for subfolder in folder_data['sub_folders']:
    subfolder_summary = process_folder_summary(subfolder)
    folder_summary.append(subfolder_summary)
  return folder_summary

# def process_root_and_second_root_level_folders(folder_data:dict):
#   folder_summary = []
#   folder_summary.append({"folder_name": folder_data['folder_name'], "summary": folder_data['summary']})
#   for subfolder in folder_data['sub_folders']:
#      if not subfolder['folder_name'].startswith('.'):
#         folder_summary.append({"folder_name": subfolder['folder_name'], "summary": subfolder['summary']})
#   return folder_summary


def get_custom_content(folder_data:dict, git_url:str, task_id:str):
    """
    Read the root level file summary, foldr summary and iteratively next level file summary and folder summary
    """
    #let first gather the root level file summary and folder summary
    summary = []
    summary.append({"folder_name": folder_data['folder_name'], "summary": folder_data['summary']})
    for file in folder_data['files']:
        # if not file['file_name'].startswith('.'):
          file_summary, status = get_file_summary(task_id, file, git_url)
          if status==200:
            summary.append({"file_name": file, "summary": file_summary})

    for subfolder in folder_data['sub_folders']:
        if not subfolder['folder_name'].startswith('.'):
            if len(subfolder['files'])==0:
              summary.append({"folder_name": subfolder['folder_name'], "summary": subfolder['summary']})
            else:
              summary.append({"folder_name": subfolder['folder_name'], "summary": subfolder['summary']})
              for file in subfolder['files']:
                file_summary, status  = get_file_summary(task_id, file, git_url)
                if status ==200:
                  summary.append({"file_name": file, "summary": file_summary})
              break
              # folder_summary = process_folder_summary(subfolder)
              # summary.extend(folder_summary)
    return summary
  
def save_design_pattern_to_db(design_pattern:dict, task_id:str)-> bool:
    """
    Save the design pattern to the mongodb.
    """
    try:
      # client = MongoClient(config.MONGODB_CLIENT)
      db = ecg_db
      collection = db['ecg_design_patterns']
      collection.insert_one(
        {'task_id': task_id, "design_pattern": design_pattern.get('design_pattern', '')}
      )
      return True
    except Exception as e:
      print(f"Error in saving design pattern to db: {e}")
      return False
    # finally:
    #   client.close()



def generate_design_pattern(task_id:str, data:dict):
    """
    Generate design pattern for the project.
    Args:
        task_id (str): Task ID
        data (dict): Folder data
    """
    try:
      # print("### Generating design pattern")
      # print("*/"*50)
      # Get the folder data
      folder_data = data[0]['folder_data']
      git_url = data[0]['git_url']
      folder_summary = get_custom_content(folder_data, git_url, task_id)

      # Get the design pattern
      design_pattern = get_design_pattern(folder_summary)
      save_design_pattern_to_db(design_pattern, task_id)
    except Exception as e:
      raise ValueError(f"Error in generating design pattern: {e}") from e