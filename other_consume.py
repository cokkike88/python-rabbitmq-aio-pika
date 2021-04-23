import asyncio
import aioboto3
import rapidjson
import logging
import coloredlogs
from typing import Callable, Dict, Any, Awaitable
from aio_pika import ExchangeType, connect_robust
from dataclasses import dataclass
from os import environ
from dynamodb_json import json_util as dynamodb_json

DATA_TABLE = environ.get('DATA_TABLE', 'hc-qa-lead-validation-worker-data-capture-dynamodb-table')

coloredlogs.install()


async def main():
    async with aioboto3.resource('dynamodb') as dynamo:
        dynamo_table = await dynamo.Table(DATA_TABLE)

        connection = await connect_robust("amqp://guest:guest@localhost/")

        _channel = await connection.channel()
        _exchange = await _channel.declare_exchange('hcTest', ExchangeType.TOPIC)

        # Create the queues
        _queue_get = await _channel.declare_queue('hcTest-get-queue')
        _queue_save = await _channel.declare_queue('hcTest-save-queue')

        # Bind the queues
        await _queue_get.bind(_exchange, 'hcTest-get')
        await _queue_save.bind(_exchange, 'hcTest-save')

        topic_queues = []
        topic_queues.append(MessageProcessor(_queue_get, process_get_event))
        topic_queues.append(MessageProcessor(_queue_save, process_save_event))

        # Call the functions for each event
        await asyncio.wait(
            [asyncio.create_task(proccess_events(message_process.queue, message_process.fn,
                                                 dynamo_table)) for message_process in
             topic_queues])


async def proccess_events(queue, fn: Callable[[Any], Awaitable], dynamo_table):
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                logging.info('Lead Validation - Produce Message')
                body = rapidjson.loads(message.body)
                await fn(body, dynamo_table)


# Proccess to events
async def process_get_event(body, dynamo_table):
    try:
        logging.info("****************")
        logging.info(body)
        session_id = body['session_id']
        res = await dynamo_table.get_item(
            Key={
                'session_id': session_id
            }
        )
        response = dynamodb_json.loads(res)
        item = response.get('Item')
        logging.info(item)
    except Exception as ex:
        logging.exception(ex)


async def process_save_event(body, dynamo_table):
    try:
        res = await dynamo_table.scan()
        response = dynamodb_json.loads(res)
        item = response.get('Items')
        logging.info(item)
    except Exception as ex:
        logging.exception(ex)

@dataclass
class MessageProcessor:
    queue: 'aio_pika.queue.Queue'
    fn: Callable[[Dict[str, Any]], Awaitable]


if __name__ == '__main__':
    logging.info("[*] Waiting for messages. To exit press CTRL+C")
    asyncio.run(main())