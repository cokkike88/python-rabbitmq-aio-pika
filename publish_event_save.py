import asyncio
import rapidjson
from aio_pika import connect, Message, ExchangeType


async def main(loop):
    # Perform connection
    connection = await connect(
        "amqp://guest:guest@localhost/", loop=loop
    )

    # Creating a channel
    channel = await connection.channel()
    routing_key = "hcTest-save"
    exchange_ = await channel.declare_exchange('hcTest', ExchangeType.TOPIC)
    message = {
        "movie_id": 4,
        "name": "Civil War",
        "release_date": "2016-04-18",
        "cast": [
            "Chris Evans",
            "Robert Downey Jr.",
            "Sebastian Stan"
        ]
    }

    # Sending the message
    await exchange_.publish(
        Message(body=rapidjson.dumps(message).encode()),
        routing_key=routing_key
    )
    print(" [x] Sent message'")
    await connection.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))