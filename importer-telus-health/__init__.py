import logging
import requests
import os
import csv
from bs4 import BeautifulSoup as soup
from datetime import datetime

import azure.functions as func

API_KEY = f'Bearer {os.environ.get("API_KEY")}'
BASE_URL = os.environ.get('BASE_URL')
VHC_ORG = os.environ.get('ORG')

with open('list.csv', newline='') as pharma:
    pharma_reader = csv.reader(pharma)
    next(pharma_reader)
    pharmacies = [i for i in pharma_reader]

def request_path(path):
    return f'https://{BASE_URL}/api/v1/{path}'

def get_telus_pharm_avail(uuid):
    # get the html and parse it using beautiful soup
    url = f'https://pharmaconnect.ca/Appointment/{uuid}/Slots?serviceType=ImmunizationCovid'
    html = requests.get(url)
    html_soup = soup(html.text, 'html.parser')
    # find the dates currently on the page, and add them to a list
    html_dates = html_soup.findAll('div', class_='b-days-selection appointment-availability__days-item')
    return bool(html_dates)

def get_location(uuid):
    url = request_path(f'locations/external/{uuid}')
    response = requests.get(url, headers={'accept': 'application/json'})
    if response.status_code != 200:
        return None
    return response.json()['id']

def create_location(uuid, name, address, postal_code, province):
    data = {
        'name': name,
        'postcode': postal_code,
        'external_key': uuid,
        'line1': address,
        'active': 1,
        'url': f'https://pharmaconnect.ca/Appointment/{uuid}/book/serviceType=ImmunizationCovid',
        'organization': VHC_ORG,
        'province': province
    }

    headers = {'Authorization': API_KEY, 'Content-Type': 'application/json'}
    location_post = requests.post(request_path('locations/expanded/'), headers=headers, json=data)
    logging.info(location_post.status_code)
    logging.info(location_post.request.url)
    logging.info(location_post.request.headers)
    logging.info(location_post.request.body)
    logging.info(location_post.text)
    logging.info(location_post.content)
    location_id = location_post.json()['id']
    return location_id

def get_availability(location):
    params = {
        'locationID': location,
        'min_date': str(datetime.now().date())
    }
    logging.info(params)
    url = request_path(f'vaccine-availability/location/')
    response = requests.get(url, params=params)
    if response.status_code != 200:
        logging.info(response.json())
        return None
    logging.info(response.json())
    availabilities = response.json()
    if len(availabilities) > 0:
        return availabilities[0]['id']
    return None

def create_availability(location, available):
    date = str(datetime.now().date())+'T00:00:00Z'
    vacc_avail_body = {
        "numberAvailable": available,
        "numberTotal": available,
        "vaccine": 1,
        "inputType": 1,
        "tags": "",
        "location": location,
        "date": date
    }
    
    vacc_avail_headers = {'accept': 'application/json', 'Authorization': API_KEY, 'Content-Type':'application/json'}
    response = requests.post(request_path('vaccine-availability'), headers=vacc_avail_headers, json=vacc_avail_body)
    logging.info(f'create_availability: {response.status_code}')
    return response.json()['id']

def update_availability(id, location, available):
    date = str(datetime.now().date())+'T00:00:00Z'
    vacc_avail_body = {
        "numberAvailable": available,
        "numberTotal": available,
        "vaccine": 1,
        "inputType": 1,
        "tags": "",
        "location": location,
        "date": date
    }
    
    vacc_avail_headers = {'accept': 'application/json', 'Authorization': API_KEY, 'Content-Type':'application/json'}
    response = requests.put(request_path(f'vaccine-availability/{id}'), headers=vacc_avail_headers, json=vacc_avail_body)
    logging.info(f'update_availability: {response.status_code}')
    return response.json()['id']


def get_or_create_location(uuid, name, address, postal_code, province):
    location = get_location(uuid)
    if location is None:
        logging.info('Creating Location')
        location = create_location(uuid, name, address, postal_code, province)
    return location

def create_or_update_availability(location, available):
    availability = get_availability(location)
    if availability is None:
        logging.info('Creating Availability')
        availability = create_availability(location, available)
    else:
        logging.info(f'Updating Availability: {availability}')
        availability = update_availability(availability, location, available)
    return availability


def main(mytimer: func.TimerRequest) -> None:
    for i in pharmacies:
        store_name = i[0]
        address = i[1]
        postal_code = i[2]
        province = i[4]
        uuid = i[5]

        if not postal_code:
            continue

        logging.info(f'Location: {uuid} {postal_code}')
        location_id = get_or_create_location(uuid, store_name, address, postal_code, province)
        available = get_telus_pharm_avail(uuid)
        logging.info(f'Availability: {available}')
        create_or_update_availability(location_id, available)
        
