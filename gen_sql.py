import aiofiles
import asyncio


def gen_update_sql(apid, code, timezone):
    template = 'UPDATE flight_airport SET apid = "{0}", timezone = "{2}" WHERE gwl = "{1}"\n'
    return template.format(apid, code, timezone)


async def main(inputfile, outputfile):
    async with aiofiles.open(inputfile, mode='r') as f_input:
        async with aiofiles.open(outputfile, mode='w') as f_output:
            async for line in f_input:
                apid, code, timezone = line.strip().split('\t')
                await f_output.write(gen_update_sql(apid, code, timezone))


inputfile = 'output.dat'
outputfile = 'output.sql'

loop = asyncio.get_event_loop()
loop.run_until_complete(main(inputfile, outputfile))
