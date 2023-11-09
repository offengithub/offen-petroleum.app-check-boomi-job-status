"""
Template Component main class.

"""
import csv
import logging
from datetime import datetime
import requests
import json
import os 
import xml.etree.ElementTree as ET
from keboola.component.exceptions import UserException
from keboola.component.base import ComponentBase
import json 
import xmltodict



# configuration variables
KEY_USERNAME = 'username'
KEY_PASSWORD = 'password'
KEY_PROCESS_ID = 'process_id'
KEY_ATOM_ID = 'atom_id'
KEY_URL = 'url'
KEY_START_TIME = 'start_time'
KEY_END_TIME = 'end_time'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME,KEY_PASSWORD,KEY_PROCESS_ID,KEY_ATOM_ID, KEY_URL, KEY_START_TIME, KEY_END_TIME]
REQUIRED_IMAGE_PARS = []


def query_execution_status(url,username, password, process_id, atom_id, start_time, end_time):
    """
    Queries the execution status of a Snowflake integration process in Boomi Atomsphere.

    :param username: The username for basic authentication.
    :param password: The password for basic authentication.
    :param process_id: The unique identifier for the process.
    :param atom_id: The unique identifier for the atom.
    :param start_time: The start time for the query range (ISO 8601 format).
    :param end_time: The end time for the query range (ISO 8601 format).
    :return: The response from the API.
    """

    # The URL for the API endpoint
    url = url

    # Construct the XML body for the request
    raw_xml = f"""
    <QueryConfig xmlns="http://api.platform.boomi.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <QueryFilter>
            <expression operator="and" xsi:type="GroupingExpression">
                <nestedExpression operator="BETWEEN" property="executionTime" xsi:type="SimpleExpression">
                    <argument>{start_time}</argument>
                    <argument>{end_time}</argument>
                </nestedExpression>
                <nestedExpression operator="EQUALS" property="processId" xsi:type="SimpleExpression">
                    <argument>{process_id}</argument>
                </nestedExpression>
                <nestedExpression operator="EQUALS" property="atomId" xsi:type="SimpleExpression">
                    <argument>{atom_id}</argument>
                </nestedExpression>
            </expression>
        </QueryFilter>
    </QueryConfig>
    """

    # Perform the POST request with Basic Auth
    response = requests.post(url, auth=(username, password), headers={'Content-Type': 'application/xml'}, data=raw_xml.strip())

    # Check the response status code
    
    if response.status_code != 200:
        logging.error("Request failed with status code: %s, response: %s", response.status_code, response.text)
        raise UserException(f"Failed to trigger the Boomi job, response code: {response.status_code}")
    '''
    # Parse the XML response
    try:
        root = ET.fromstring(response.text)
        # Check if the response contains ExecutionRequest and requestId
        if root.tag.endswith('ExecutionRequest') and 'requestId' in root.attrib:
            logging.info("Boomi job triggered successfully with requestId: %s", root.attrib['requestId'])
        else:
            raise UserException("Boomi API did not return a success message.")
    except ET.ParseError as e:
        logging.error("Failed to parse XML response: %s", e)
        raise UserException("Failed to parse the Boomi API response.")
    '''
    # Convert XML to a Python dictionary
    dict_data = xmltodict.parse(response.text)

    # Convert the Python dictionary to a JSON string
    response_json_data = json.dumps(dict_data, indent=4)

    return response_json_data

class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self) -> None:

        """Runs the component.

        Validates the configuration parameters and triggers a Boomi job.
        """

       # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        
        username = self.configuration.parameters.get(KEY_USERNAME)
        print(username)
        password = self.configuration.parameters.get(KEY_PASSWORD)
        process_id = self.configuration.parameters.get(KEY_PROCESS_ID)
        url = self.configuration.parameters.get(KEY_URL)
        atom_id = self.configuration.parameters.get(KEY_ATOM_ID)
        start_time = self.configuration.parameters.get(KEY_START_TIME)
        end_time= self.configuration.parameters.get(KEY_END_TIME)

        
        response=query_execution_status(url,username, password, process_id, atom_id,start_time, end_time)
        if response:
            print(response)
        #self.write_manifest(out_table)

"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception("User configuration error: %s", exc)
        exit(1)
    except Exception as exc:
        logging.exception("Unexpected error: %s", exc)
        exit(2)
