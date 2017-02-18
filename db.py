# -*- coding: utf-8 -*-

import asyncio
import aioodbc
#import uvloop
#asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def test_db():
    dsn = 'Driver=SQLite;Database=sqlite.db'
    conn = await aioodbc.connect(dsn=dsn, loop=loop)

    cur = await conn.cursor()
    await cur.execute("SELECT 42;")
    r = await cur.fetchall()
    print(r)
    await cur.close()
    await conn.close()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_db())


if __name__ == '__main__':
    main()
