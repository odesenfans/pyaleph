import asyncio
import logging

from aleph.exceptions import InvalidMessageError
from .service import IpfsService
from ...toolkit.timestamp import utc_now

LOGGER = logging.getLogger("IPFS.PUBSUB")


# TODO: add type hint for message_processor, it currently causes a cyclical import
async def incoming_channel(
    ipfs_service: IpfsService, topic: str, message_handler
) -> None:
    from aleph.network import decode_pubsub_message

    while True:
        try:
            async for mvalue in ipfs_service.sub(topic):
                try:
                    message_dict = await decode_pubsub_message(mvalue["data"])
                    await message_handler.add_pending_message(
                        message_dict=message_dict, reception_time=utc_now()
                    )
                except InvalidMessageError:
                    LOGGER.warning(f"Invalid message {mvalue}")
        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting in 2 seconds...")
            await asyncio.sleep(2)
