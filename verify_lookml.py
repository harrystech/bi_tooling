import looker
import os
import requests
import sys
import time
import queue
import logging

import numpy as np

from concurrent.futures import ThreadPoolExecutor, wait

IGNORABLE_SQL_ERROR = 'could not devise a query plan'


def get_token_client():
    """
    Authenticate user credentials and return token

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
    Create a list of dimensions and measures from the given model-explore
    """
    dimensions = [dimension.name for dimension in model_explore_body.fields.dimensions]
    measures = [measure.name for measure in model_explore_body.fields.measures]
    return dimensions + measures


def check_for_query_error(query_id, token, timeout=3):
    """
    Check for Looker error in query results

    A timeout exception implies that the query is running successfully. The errors we care about
    are ones that fail immediately
    """
    try:
        # Using requests.get instead of API client in order to specify timeout
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
            return element['looker_error']

    return None


def process_branch(query_client, token, branch,
                   branch_queue, happy_queue, quasi_happy_queue, broken_field_queue, processed_field_queue):
    """
    Divide and conquer by splitting fields until field is proven innocent or guilty

    1: If error and single field, you've found the guilty party
    2: If error and not single field, you divide the fields and process both later
    3: If no error and all original fields are present, the original model-explore is error free
    4: If no error and some original fields are present, the original model-explore has a broken field
    """
    # Generate Query-ID
    query_id = generate_query_id(query_client, branch)

    # Check for query error
    message = check_for_query_error(query_id, token)

    if message and IGNORABLE_SQL_ERROR not in message:
        # If there is one field left then add it to error field queue
        model_name, explore_name, fields, starting_field_count = branch
        if len(fields) == 1:  # fields = ['xxxx.yyyy', 'xxxx.yyyy']
            broken_field_queue.put(branch)
            for field in fields:
                processed_field_queue.put(field)
        # Else split the field into two and put both back on the queue
        else:
            divided_branches = divide_branch(branch)
            for divided_branch in divided_branches:
                branch_queue.put(divided_branch)
    else:
        model_name, explore_name, fields, starting_field_count = branch
        for field in fields:
            processed_field_queue.put(field)
        # If length of field equals all fields
        if len(fields) == starting_field_count:
            happy_queue.put(branch)
        # If length of field is less than all fields
        else:
            quasi_happy_queue.put(branch)


def divide_branch(branch):
    """
    Split work into two
    """
    model_name, explore_name, fields, starting_field_count = branch
    left_fields = fields[:len(fields)//2]
    right_fields = fields[len(fields)//2:]
    return [[model_name, explore_name, left_fields, starting_field_count],
            [model_name, explore_name, right_fields, starting_field_count]]


def generate_query_id(query_client, branch):
    """
    Build the body of the query and return its ID
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

    # Holds queries that need to be checked for errors
    branch_queue = queue.Queue()
    # Holds model-explores if all fields are error-free
    happy_queue = queue.Queue()
    # Holds model-explores if some fields are error-free
    quasi_happy_queue = queue.Queue()
    # Holds fields that produce errors
    broken_field_queue = queue.Queue()
    # Holds all fields that have been checked from the branch_queue
    processed_field_queue = queue.Queue()

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
                executor.submit(process_branch, query_client, token, branch,
                                branch_queue, happy_queue, quasi_happy_queue, broken_field_queue, processed_field_queue)
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
