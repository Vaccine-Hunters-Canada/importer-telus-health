import logging
import os
import csv
from bs4 import BeautifulSoup as soup
from datetime import datetime
from dotenv import load_dotenv
import asyncio
import aiohttp

import azure.functions as func

load_dotenv()
API_KEY = f'Bearer {os.getenv("API_KEY")}'
BASE_URL = os.getenv('BASE_URL')
VHC_ORG = os.getenv('ORG')

with open('list.csv', newline='') as pharma:
    pharma_reader = csv.reader(pharma)
    next(pharma_reader)
    pharmacies = [i for i in pharma_reader]

def request_path(path):
    return f'https://{BASE_URL}/api/v1/{path}'

async def get_telus_pharm_avail(session, uuid):
    # get the html and parse it using beautiful soup
    url = f'https://pharmaconnect.ca/Appointment/{uuid}/Slots?serviceType=ImmunizationCovid'
    html = await session.get(url)
    html_text = await html.text()
    html_soup = soup(html_text, 'html.parser')
    # find the dates currently on the page, and add them to a list
    html_dates = html_soup.findAll('div', class_='b-days-selection appointment-availability__days-item')
    return bool(html_dates)

async def get_location(session, uuid):
    url = request_path(f'locations/external/{uuid}')
    response = await session.get(url, headers={'accept': 'application/json'})
    data = None
    try:
        data = await response.json()
    except aiohttp.client_exceptions.ContentTypeError: # if location does not exist
        if not data:
            return None
    return data['id']

async def create_location(session, uuid, name, address, postal_code, province):
    data = {
        'name': name,
        'postcode': postal_code,
        'external_key': uuid,
        'line1': address,
        'active': 1,
        'url': f'https://pharmaconnect.ca/Appointment/{uuid}/Book/ImmunizationCovid',
        'organization': VHC_ORG,
        'province': province
    }

    headers = {'Authorization': API_KEY, 'Content-Type': 'application/json'}
    location_post = await session.post(request_path('locations/expanded'), headers=headers, json=data)
    location_id = await location_post.text()
    logging.info(location_id)
    return location_id

async def get_availability(session, location):
    params = {
        'locationID': location,
        'min_date': str(datetime.now().date())
    }
    logging.info(params)
    url = request_path(f'vaccine-availability/location/')
    response = await session.get(url, params=params)
    
    if response.status != 200:
        logging.info(await response.json())
        return None
    logging.info(await response.json())
    availabilities = await response.json()
    if len(availabilities) > 0:
        return availabilities[0]['id']
    return None

async def create_availability(session, location, available):
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
    response = await session.post(request_path('vaccine-availability'), headers=vacc_avail_headers, json=vacc_avail_body)
    logging.info(f'create_availability: {response.status}')
    data = await response.json()
    return  data['id']

async def update_availability(session, id, location, available):
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
    response = await session.put(request_path(f'vaccine-availability/{id}'), headers=vacc_avail_headers, json=vacc_avail_body)
    logging.info(f'update_availability: {response.status}')
    data = await response.json()
    return data['id']


async def get_or_create_location(session, uuid, name, address, postal_code, province):
    location = await get_location(session, uuid)
    if location is None:
        logging.info('Creating Location')
        location = await create_location(session, uuid, name, address, postal_code, province)
    return location

async def create_or_update_availability(session, location, available):
    availability = await get_availability(session, location)
    if availability is None:
        logging.info('Creating Availability')
        availability = await create_availability(session, location, available)
    else:
        logging.info(f'Updating Availability: {availability}')
        availability = await update_availability(session, availability, location, available)
    return availability


async def main(mytimer: func.TimerRequest) -> None:
    async with aiohttp.ClientSession() as session:
        for i in pharmacies:

            store_name = i[0]
            address = i[1]
            postal_code = i[2]
            province = i[4]
            uuid = i[5]

            if not postal_code:
                continue

            logging.info(f'Location: {uuid} {postal_code}')
            location_id = await get_or_create_location(session, uuid, store_name, address, postal_code, province)
            available = await get_telus_pharm_avail(session, uuid)
            logging.info(f'Availability: {available}')
            await create_or_update_availability(session, location_id, available)

        
