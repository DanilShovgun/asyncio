import asyncio
import aiohttp
import asyncpg


async def create_table():
    conn = await asyncpg.connect(database='starwars', user='user', password='password')
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        id integer PRIMARY KEY,
        name text,
        birth_year text,
        eye_color text,
        gender text,
        hair_color text,
        height text,
        homeworld text,
        mass text,
        skin_color text,
        films text,
        species text,
        starships text,
        vehicles text
    )
    """)
    await conn.close()


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()


async def extract_data(session, id):
    character_data = await fetch(session, f'https://swapi.dev/api/people/{id}/')
    character_data['films'] = ', '.join([await (await fetch(session, film_url))['title'] for film_url in character_data['films']])
    character_data['species'] = ', '.join([await (await fetch(session, species_url))['name'] for species_url in character_data['species']])
    character_data['starships'] = ', '.join([await (await fetch(session, starship_url))['name'] for starship_url in character_data['starships']])
    character_data['vehicles'] = ', '.join([await (await fetch(session, vehicle_url))['name'] for vehicle_url in character_data['vehicles']])
    return character_data


async def load_data(session, id):
    conn = await asyncpg.connect(database='starwars', user='user', password='password')
    character_data = await extract_data(session, id)
    fields = ('id', 'name', 'birth_year', 'eye_color', 'gender', 'hair_color', 'height', 'homeworld', 'mass', 'skin_color', 'films', 'species', 'starships', 'vehicles')
    await conn.execute("""
        INSERT INTO characters ({})
        VALUES ({})
        ON CONFLICT (id) DO NOTHING
    """.format(
        ','.join(fields),
        ','.join(f'${i+1}' for i in range(len(fields)))
    ), [character_data[field] for field in fields])
    await conn.close()


async def main():
    await create_table()
    async with aiohttp.ClientSession() as session:
        for character_id in range(1, 100):
            try:
                await load_data(session, character_id)
            except Exception as e:
                print(f'Ошибка при обработке ID {character_id}:', e)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
