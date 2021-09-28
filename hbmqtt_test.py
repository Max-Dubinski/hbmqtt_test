import logging
import asyncio
import random

from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.mqtt.constants import QOS_1, QOS_2

logger = logging.getLogger(__name__)

async def test_sub():
    C = MQTTClient()
    await C.connect('mqtt://localhost:1883/')
    # Subscribe to '$SYS/broker/uptime' with QOS=1
    await C.subscribe([
        ('test/#', QOS_1),
    ])
    logger.info("Subscribed")
    try:
        for i in range(1, 100):
            message = await C.deliver_message()
            packet = message.publish_packet
            print("%d: %s => %s" % (i, packet.variable_header.topic_name, str(packet.payload.data)))
        await C.unsubscribe(['$SYS/broker/uptime', '$SYS/broker/load/#'])
        logger.info("UnSubscribed")
        await C.disconnect()
    except ClientException as ce:
        logger.error("Client exception: %s" % ce)


if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    formatter = "%(message)s"
    try:
        logging.basicConfig(level=logging.DEBUG, format=formatter)
        loop = asyncio.get_event_loop()
        sub_task = loop.create_task(test_sub())
        while (True):
            print("Waiting for client")
            loop.run_until_complete(asyncio.sleep(random.random()))
    except Exception as e:
        logger.error(e)
        loop.close()

