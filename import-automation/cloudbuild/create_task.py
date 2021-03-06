# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Creates a Cloud Tasks task that ships information about a GitHub commit
to the executor.
"""

import os
import json

from google.cloud import tasks_v2

# These fields must present as environmental variables.
# PR_NUMBER must be an integer.
# Otherwise, the script will fail.
TASK_BODY_FIELDS = [
    'COMMIT_SHA',
    'REPO_NAME',
    'BRANCH_NAME',
    'HEAD_BRANCH',
    'BASE_BRANCH',
    'PR_NUMBER'
]


def create_body():
    """Creates the task body for a Cloud Tasks task.

    Specifically, this function looks for the value of every field defined
    in TASK_BODY_FIELDS in the environment variables.

    PR_NUMBER is converted to an integer.

    Returns:
        The task body as a dict.
    """
    task_body = {}
    for field in TASK_BODY_FIELDS:
        task_body[field] = os.environ[field]
    task_body['PR_NUMBER'] = int(task_body['PR_NUMBER'])
    return task_body


def create_task(task_body, project_id, location_id, queue_name,
                service, endpoint):
    """Creates a Google Cloud Tasks App Engine task.

    The App Engine app that handles the task must be under the same Google
    Cloud project as the task queue.

    The Cloud Tasks Client will attempt to infer credentials based on
    host environment. See
    https://cloud.google.com/docs/authentication/production#finding_credentials_automatically.

    Args:
        task_body: Request body of the task as a dict that
            will be serialized into json.
        project_id: ID of the Google Cloud project as a string.
        location_id: ID of the location where the task queue is hosted.
        queue_name: Name of the task queue.
        service: Name of the App Engine service that will handle the task.
        endpoint: Relative URL of the App Engine task handler endpoint.

    Returns:
        The created task as a dict.
        The CloudTasksClient used to create the task.
    """
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project_id, location_id, queue_name)
    body = json.dumps(task_body)
    task = {
        'app_engine_http_request': {
            'app_engine_routing': {
                'service': service
            },
            'http_method': 'POST',
            'relative_uri': endpoint,
            'body': body.encode(),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    }
    client.create_task(parent, task)
    return task, client


def main():
    """
    Creates a Cloud Tasks task that ships information about a GitHub commit
    to the executor.
    """
    # TASK_PROJECT_ID, TASK_LOCATION_ID, TASK_QUEUE_NAME, HANDLER_SERVICE, and
    # HANDLER_URI must be present as environmental variables.
    # Otherwise, the script will fail.
    create_task(
        task_body=create_body(),
        project_id=os.environ['TASK_PROJECT_ID'],
        location_id=os.environ['TASK_LOCATION_ID'],
        queue_name=os.environ['TASK_QUEUE_NAME'],
        service=os.environ['HANDLER_SERVICE'],
        endpoint=os.environ['HANDLER_URI'])


if __name__ == '__main__':
    main()
