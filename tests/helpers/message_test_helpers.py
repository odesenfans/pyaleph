import json
from typing import Dict, List, Optional, Union

from aleph_message.models import ItemType, MessageConfirmation

from aleph.db.models import MessageDb, PendingMessageDb
from aleph.schemas.message_content import MessageContent, ContentSource
from aleph.schemas.pending_messages import parse_message
from aleph.schemas.validated_message import validate_pending_message


def make_validated_message_from_dict(
    message_dict: Dict,
    raw_content: Optional[Union[str, bytes]] = None,
    confirmations: Optional[List[MessageConfirmation]] = None,
) -> MessageDb:
    """
    Creates a validated message instance from a raw message dictionary.
    This is helpful to easily import fixtures from an API or the DB and transform
    them into a valid object.

    :param message_dict: The raw message dictionary.
    :param raw_content: The raw content of the message, as a string or bytes.
    :param confirmations: List of confirmations, if any.
    """

    if raw_content is None:
        assert message_dict["item_type"] == ItemType.inline
        raw_content = message_dict["item_content"]

    pending_message = PendingMessageDb.from_message_dict(message_dict)
    return MessageDb.from_pending_message(
        pending_message=pending_message,
        content_dict=json.loads(raw_content),
        content_size=len(raw_content),
    )
