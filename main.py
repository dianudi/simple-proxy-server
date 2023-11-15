import asyncio
from asyncio import StreamReader, StreamWriter
import logging
from re import search

logging.basicConfig(level=logging.INFO)

# forward stream


async def pipe(r: StreamReader, w: StreamWriter):
    try:
        while not r.at_eof():
            w.write(await r.read(4096))
    finally:
        w.close()

# handle every connection


async def conn_handler(lr: StreamReader, lw: StreamWriter):
    data = (await lr.read(4096)).decode()
    logging.debug(f'Got connection from {lw.get_extra_info("peername")[0]}')
    logging.debug(data)
    try:
        # for HTTPS or any except HTTP
        if data.startswith('CONNECT'):
            host, port = data.splitlines()[0].split(' ')[1].split(':')
            rr, rw = await asyncio.open_connection(host, port)
            lw.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await lw.drain()
            await asyncio.gather(pipe(lr, rw), pipe(rr, lw))
        # for HTTP
        else:
            host = search(r"Host: (.+)\r\n", data).group(1)
            rr, rw = await asyncio.open_connection(host, 80)
            rw.write(bytes(data, 'utf-8'))
            await rw.drain()
            await asyncio.gather(pipe(lr, rw), pipe(rr, lw))
    except ConnectionResetError as e:
        logging.error(e)
    except Exception as e:
        logging.error(e)
    finally:
        lw.close()


async def main():
    server = await asyncio.start_server(conn_handler, '0.0.0.0', 5000)
    logging.info(
        f'Server ready listen at port {server.sockets[0].getsockname()[1]}')
    await server.serve_forever()
try:
    asyncio.run(main())
except KeyboardInterrupt:
    exit(1)
except Exception as e:
    logging.error(e)
