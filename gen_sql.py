import aiofiles
import asyncio


def gen_update_sql(code, timezone):
    template = 'UPDATE flight_airport SET timezone = "{1}" WHERE gwl = "{0}"\n'
    return template.format(code, timezone)


async def main():
    input_file = "output.txt"
    output_file = "output.sql"
    async with aiofiles.open(input_file, mode='r') as f_input:
        async with aiofiles.open(output_file, mode='w') as f_output:
            async for line in f_input:
                code, timezone = line.strip().split('\t')
                await f_output.write(gen_update_sql(code, timezone))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
