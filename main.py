import aiohttp
import asyncio
from db import DBWorker, dispose_engine


API_URL = 'https://swapi.dev/api/people/'
CHUNK_SIZE = 20
db_worker = DBWorker()


async def get_page_data(url, page_number='', field_name=None):
    params = None
    async with aiohttp.ClientSession() as session:
        if page_number:
            params = {'page': page_number}
        async with session.get(url, params=params) as result:
            data = await result.json()
    if field_name:
        return data[field_name]
    return data


def get_character_field_name_list(field, api_field='name'):
    result = []
    for url in field:
        result.append(asyncio.create_task(get_page_data(url, field_name=api_field)))
    return result


async def store_character(character):
    films = get_character_field_name_list(character['films'], api_field='title')
    species = get_character_field_name_list(character['species'])
    starships = get_character_field_name_list(character['starships'])
    vehicles = get_character_field_name_list(character['vehicles'])

    character['films'] = await asyncio.gather(*films)
    character['species'] = await asyncio.gather(*species)
    character['starships'] = await asyncio.gather(*starships)
    character['vehicles'] = await asyncio.gather(*vehicles)

    character['films'] = ','.join(character['films'])
    character['species'] = ','.join(character['species'])
    character['starships'] = ','.join(character['starships'])
    character['vehicles'] = ','.join(character['vehicles'])
    await db_worker.save_character(character)
    print(character)


async def main():
    await db_worker.begin()
    page = 1
    while True:
        page_data = await get_page_data(API_URL, page)
        if not page_data.get('next'):
            break

        tasks = [asyncio.create_task(store_character(character)) for character in page_data['results']]
        await asyncio.gather(*tasks)
        page += 1
    await dispose_engine()

# нужно на винде
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())