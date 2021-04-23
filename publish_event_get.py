import asyncio
from aio_pika import connect, Message, ExchangeType
import rapidjson


async def main(loop):
    # Perform connection
    connection = await connect(
        "amqp://guest:guest@localhost/", loop=loop
    )

    # Creating a channel
    channel = await connection.channel()
    routing_key = "hcTest-get"
    exchange_ = await channel.declare_exchange('hcTest', ExchangeType.TOPIC)
    message = {
        "movie_id": 1,
        "session_id": "20210405232313.b5d3a90bc13a"
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