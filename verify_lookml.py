import looker
import os
import requests
import sys
import time
import queue
import logging

import numpy as np

from concurrent.futures import ThreadPoolExecutor, wait


def get_token_client():
    """
    { LOOKER_BASE_URL, LOOKER_API_ID, LOOKER_API_SECRET } need to be set as environment variables
    """
    unauthenticated_client = looker.ApiClient(os.environ.get('LOOKER_BASE_URL'))
    unauthenticated_authApi = looker.ApiAuthApi(unauthenticated_client)

    token = unauthenticated_authApi.login(
        client_id=os.environ.get('LOOKER_API_ID'),
        client_secret=os.environ.get('LOOKER_API_SECRET'))

    client = looker.ApiClient(os.environ.get('LOOKER_BASE_URL'), 'Authorization', 'token ' + token.access_token)
    return token, client


def get_fields(model_explore_body):
    """
    """
    dimensions = [dimension.name for dimension in model_explore_body.fields.dimensions]
    measures = [measure.name for measure in model_explore_body.fields.measures]
    return dimensions + measures


def check_for_query_error(query_id, token, timeout=3):
    """
    """
    try:
        endpoint = 'queries/' + str(query_id) + '/run/json'
        r = requests.get(
            os.environ.get('LOOKER_BASE_URL') + endpoint,
            headers={'Authorization': 'token ' + token.access_token},
            timeout=timeout
        )
        results = r.json()
    except requests.exceptions.Timeout:  # A timeout exception is OK
        results = []

    for element in results:
        if 'looker_error' in element:
            return (True, element['looker_error'])

    return (False, '')


def process_branch(query_client, token, branch, branch_queue, happy_queue, quasi_happy_queue, broken_field_queue, processed_field_queue):
    """
    """
    # Generate Query-ID
    query_id = generate_query_id(query_client, branch)

    # Check for query error
    errored, message = check_for_query_error(query_id, token)

    ignorable_sql_error = 'could not devise a query plan'
    if errored and ignorable_sql_error not in message:
        # If there is one field left then add it to error field queue
        model_name, explore_name, fields, starting_field_count = branch
        if len(fields) == 1:  # fields = ['xxxx.yyyy', 'xxxx.yyyy']
            broken_field_queue.put(branch)
            [processed_field_queue.put(field) for field in fields]
        # Else split the field into two and put both back on the queue
        else:
            divided_branches = divide_branch(branch)
            for divided_branch in divided_branches:
                branch_queue.put(divided_branch)
    else:
        model_name, explore_name, fields, starting_field_count = branch
        [processed_field_queue.put(field) for field in fields]
        # If length of field equals all fields
        if len(fields) == starting_field_count:
            happy_queue.put(branch)
        # If length of field is less than all fields
        else:
            quasi_happy_queue.put(branch)


def divide_branch(branch):
    """
    """
    model_name, explore_name, fields, starting_field_count = branch
    left_branch = fields[:len(fields)//2]
    right_branch = fields[len(fields)//2:]
    return [[model_name, explore_name, left_branch, starting_field_count], [model_name, explore_name, right_branch, starting_field_count]]


def generate_query_id(query_client, branch):
    """
    """
    model_name, explore_name, fields, starting_field_count = branch

    query_body = {'limit': '1'}  # Limit the number of rows in the query
    query_body['model'] = model_name
    query_body['view'] = explore_name
    query_body['fields'] = fields

    query_response = query_client.create_query(body=query_body)
    return query_response.id


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
    token, client = get_token_client()
    model_client = looker.LookmlModelApi(client)
    query_client = looker.QueryApi(client)
    start = time.time()

    selected_models = sys.argv[1:]

    def is_checkable(model):
        return model.has_content and (not selected_models or model.name in selected_models)

    # A generator of models that contain explores with content
    models = filter(is_checkable, model_client.all_lookml_models())

    branch_queue = queue.Queue()            # Holds queries that need to be checked for errors
    happy_queue = queue.Queue()             # Holds model-explores if all fields are error-free
    quasi_happy_queue = queue.Queue()       # Holds model-explores if some fields are error-free
    broken_field_queue = queue.Queue()      # Holds fields that produce errors
    processed_field_queue = queue.Queue()   # Holds all fields that have been checked from the branch_queue
    for model in models:
        logging.info("Loading '{}' model".format(model.name))
        for explore_info in model.explores:
            if explore_info.hidden:  # Ignore all hidden explores
                continue

            logging.info("Loading '{}' explore".format(explore_info.name))

            # Create Model-Explore Body for the purposes of getting all the fields
            model_explore_body = model_client.lookml_model_explore(model.name, explore_info.name)
            model_explore_fields = get_fields(model_explore_body)
            branch_queue.put([model.name, explore_info.name, model_explore_fields, len(model_explore_fields)])

    total_starting_field_count = np.sum([count[-1] for count in list(branch_queue.queue)])

    executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix='primary')
    futures = []
    while processed_field_queue.qsize() != total_starting_field_count:
        try:
            branch = branch_queue.get(timeout=1)
            futures.append(
                executor.submit(process_branch, query_client, token, branch, branch_queue, happy_queue, quasi_happy_queue, broken_field_queue, processed_field_queue)
            )
        except queue.Empty:
            continue

    done, not_done = wait(futures)
    logging.info("All fields in queue have been processed")

    # Report out results that user cares about
    while True:
        try:
            branch = happy_queue.get(timeout=1)
            model_name, explore_name, field, starting_field_count = branch
            logging.info("No Errors detected in '{}' explore".format(explore_name))
        except queue.Empty:
            break

    while True:
        try:
            branch = broken_field_queue.get(timeout=1)
            model_name, explore_name, field, starting_field_count = branch
            logging.info("Error field: '{}' explore :'{}'".format(field[0], explore_name))
        except queue.Empty:
            break

    end = time.time()
    logging.info("Total time to completion: {0:.3f} seconds".format(end-start))


if __name__ == '__main__':
    main()
