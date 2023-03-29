# Forgets

FORGET messages are meant to make the Aleph network forget/drop one or
more messages sent previously. Users can forget any type of message,
except for FORGET messages themselves.

When a FORGET message is processed by a node, it will immediately mark the target message(s) as forgotten,
meaning that their `content` (and `item_content`) fields will be deleted.
In addition, any content related to the forgotten message(s) will be deleted, if no other message points to the same
content. For example, forgetting a STORE message will delete the associated file, if no other STORE message points
to the same file.

Furthermore, forgotten messages will no longer appear when querying messages from `/api/v0/messages.json`.

## Content format

The `content` field of a FORGET message must contain the
following fields:

- `address [str]`: The address to which the message belongs. See [permissions](../permissions.md).
- `time [float]`: The epoch timestamp of the message.
- `hashes [List[str]]`: The list of message hashes to forget
- `aggregates [List[str]]`: The list of aggregates to forget
- `reason [Optional[str]]`: An optional explanation of why the user wants to forget these hashes.

FORGET messages must specify at least one object to forget.

## Limitations

- At the moment, a user can only forget messages he sent himself.
