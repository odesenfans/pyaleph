"""
This migration fixes the "time" field of messages. It was incorrectly updated when fetching
messages from the on-chain storage.

We now store the original message time in the "time" field, and we store the reception time
of the message in the new `reception_time` field. `reception_time` will contain the time at which the node
sees a message for the first time on the network, or the block timestamp once the message
is confirmed.
"""

import logging

from configmanager import Config

from aleph.model.chains import Chain
from aleph.model.messages import Message

logger = logging.getLogger()


async def must_run_migration() -> bool:
    nb_documents = Message.collection.count_documents(
        filter={"reception_time": {"$exists": 1}}
    )
    return nb_documents == 0


async def set_timestamp_in_confirmation():
    # TODO: reset the timestamp properly by re-running all chain messages
    ...


async def create_reception_time():
    update = [{"$set": {"reception_time": "$time"}}]

    logger.info("Creating the reception_time field on all messages. This operation may take a while.")
    await Message.collection.update_many(filter={}, update=update)
    logger.info("reception_time field successfully created on all messages.")


async def update_message_time():
    filter = {"content.time": {"$exists": 1}}
    update = [{"$set": {"time": "$content.time"}}]

    logger.info("Resetting the time field on messages. This operation may take a while.")
    await Message.collection.update_many(filter=filter, update=update)
    logger.info("Reset message times to their original value.")


async def upgrade(config: Config, **kwargs):
    if await must_run_migration():
        logger.info("Messages with inconsistent times found, running migration.")
        start_height = config.ethereum.start_height.value
        await Chain.set_last_height("ETH", start_height)
    else:
        logger.info("Message times already set to the correct value, skipping migration.")

    logger.info("Some queries may take a while to execute.")

    # First, move the message time to the new reception_time field. The currently unconfirmed messages
    # on the node will have reception_time = content.time but this issue will fix itself upon
    # confirmation of these messages.
    await create_reception_time()

    # First, update all the messages that have a valid content.time field.
    # This represents 99.99% of the messages in the DB, the only exception
    # being forgotten messages.
    # await update_message_time()


def downgrade(**kwargs):
    # Nothing to do, we do not wish to revert this migration.
    pass
