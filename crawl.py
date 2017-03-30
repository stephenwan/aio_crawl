import asyncio
import aiohttp
import aiofiles
import json
import tqdm


class Resource:
    @classmethod
    def setup(cls, k, v):
        setattr(cls, k, v)

    @classmethod
    def node_url(cls, nodecode):
        return cls.node_url_pref + nodecode

    @classmethod
    def lookup_url(cls, lookupid):
        return cls.lookup_url_pref + lookupid


async def fetch(session, semaphor, url):
    async with semaphor:
        async with session.get(url) as response:
            json_result = await response.text()
            return json.loads(json_result)


async def fetch_airport_info(session, semaphor, lookupids):
    tasks = [fetch(session, semaphor, Resource.lookup_url(lookupid))
             for lookupid in lookupids]
    results = []
    for task in tqdm.tqdm(asyncio.as_completed(tasks),
                          total=len(tasks),
                          desc='fetch airport infos'.ljust(20)):
        airport_info = await task
        results.append(airport_info)
    return results


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


async def fetch_airport_lookupid(session, semaphor, codes):
    batches = batch(codes, 1)
    tasks = [fetch(session, semaphor, Resource.node_url(','.join(batch)))
             for batch in batches]
    lookupids = []
    for task in tqdm.tqdm(asyncio.as_completed(tasks),
                          total=len(tasks),
                          desc='lookup airport ids'.ljust(20)):
        nodes = await task
        nodeids = [node['OutputCode'] for node in nodes if node['Type'] == 'Airport']
        lookupids = lookupids + nodeids
    return lookupids


async def output_to_file(filename, airport_infos):
    async with aiofiles.open(filename, mode='w') as f:
        for airport_info in tqdm.tqdm(airport_infos,
                                      total=len(airport_infos),
                                      desc='output to file'.ljust(20)):
            await f.write("%s\t%s\n" % (airport_info['DisplayCode'], airport_info['OlsonTimeZoneId']))


async def read_from_file(filename):
    async with aiofiles.open(filename, mode='r') as f:
        content = await f.read()
        return content


async def setup_resource():
    async with aiofiles.open('resource.dat', mode='r') as f:
        async for line in f:
            k, v = line.strip().split('\t')
            Resource.setup(k, v)


async def main(loop, semaphor):
    await setup_resource()
    airport_codes = await read_from_file('codes.dat')
    filename = "output.txt"
    async with aiohttp.ClientSession(loop=loop) as session:
        lookupids = await fetch_airport_lookupid(session, semaphor, airport_codes.split(','))
        airport_infos = await fetch_airport_info(session, semaphor, lookupids)
        await output_to_file(filename, airport_infos)
        return airport_infos


sem = asyncio.Semaphore(5)
loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop, sem))
