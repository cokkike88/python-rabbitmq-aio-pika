import asyncio
import aio_pika
import rapidjson
import logging
import coloredlogs

coloredlogs.install()

movies = [
    {
        "movie_id": 1,
        "name": "Batman Begins",
        "release_date": "2005-06-15",
        "cast": [
            "Christian Bale",
            "Cillian Muyphy",
            "Michael Caine"
        ]
    },
    {
        "movie_id": 2,
        "name": "The Dark Knight",
        "release_date": "2008-07-18",
        "cast": [
            "Christian Bale",
            "Heath Ledger",
            "Gary Oldman"
        ]
    },
{
        "movie_id": 3,
        "name": "The Dark Knight Rises",
        "release_date": "2012-07-20",
        "cast": [
            "Christian Bale",
            "Aaron Eckhart",
            "Morgan Freeman"
        ]
    }
]

async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/", loop=loop
    )

    queue_name = "hcTest-get-queue"
    queue_name_save = "hcTest-save-queue"
    channel = await connection.channel()

    # Declaring queue
    queue = await channel.declare_queue(queue_name)
    queue_save = await channel.declare_queue(queue_name_save)
    # Exchange
    exchange = await channel.declare_exchange('hcTest', aio_pika.ExchangeType.TOPIC)

    # Binding
    await queue.bind(exchange, 'hcTest-get')
    await queue_save.bind(exchange, 'hcTest-save')
    # Consume
    await queue.consume(process_message)
    await queue_save.consume(process_message_save)


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        print("****************")
        body = rapidjson.loads(message.body)
        logging.info(body)
        movie_id = body.get('movie_id', 0)
        result = next((e for e in movies if e['movie_id'] == movie_id), {})
        logging.info(result)
        await asyncio.sleep(1)


async def process_message_save(message: aio_pika.IncomingMessage):
    async with message.process():
        print("****************")
        body = rapidjson.loads(message.body)
        logging.info(body)
        movies.append(body)
        logging.info(movies)
        await asyncio.sleep(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()