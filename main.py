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

    film_tasks = [fetch(session, film_url) for film_url in character_data['films']]
    species_tasks = [fetch(session, species_url) for species_url in character_data['species'] if 'name' in species_url]
    starship_tasks = [fetch(session, starship_url) for starship_url in character_data['starships'] if 'name' in starship_url]
    vehicle_tasks = [fetch(session, vehicle_url) for vehicle_url in character_data['vehicles'] if 'name' in vehicle_url]

    tasks = film_tasks
    if species_tasks: tasks += species_tasks
    if starship_tasks: tasks += starship_tasks
    if vehicle_tasks: tasks += vehicle_tasks

    gathered_results = await asyncio.gather(*tasks)

    films = gathered_results[:len(film_tasks)]
    species = gathered_results[len(film_tasks):len(film_tasks)+len(species_tasks)]
    starships = gathered_results[len(film_tasks)+len(species_tasks):len(film_tasks)+len(species_tasks)+len(starship_tasks)]
    vehicles = gathered_results[len(film_tasks)+len(species_tasks)+len(starship_tasks):]
    
    character_data['films'] = ', '.join([film['title'] for film in films if 'title' in film])
    character_data['species'] = ', '.join([specie['name'] for specie in species if 'name' in specie]) if species else None
    character_data['starships'] = ', '.join([starship['name'] for starship in starships if 'name' in starship]) if starships else None
    character_data['vehicles'] = ', '.join([vehicle['name'] for vehicle in vehicles if 'name' in vehicle]) if vehicles else None

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
