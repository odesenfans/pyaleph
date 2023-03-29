# Messages

All data transferred over the aleph.im network are Aleph messages and
represent the core of the Aleph networking model.

Messages can be:

- sent and received on the REST or other API interfaces
- exchanged on the peer-to-peer network
- stored on the underlying chains

## Message format

An Aleph.im message is made of multiple fields that can be split between header and content fields.

### Header fields

#### Message info fields

* `type [str]`: the type of the message. Can be one of `AGGREGATE`, `FORGET`, `PROGRAM`,`POST`, `STORE`.
* `time [float]`: Message timestamp.
* `channel [Optional[str]]`: A user-defined string defining the channel of the message. One application ideally has one channel.
* `signature [str]`: The cryptographic signature of the message. This field guarantees the authenticity of the message.

#### Sender info fields

* `sender [str]`: Cryptographic address of the sender. Ex: the user's crypto wallet address.
* `chain [str]`: The blockchain used by the sender.

### Content fields

* `item_hash [str]`: The hash of the message content. See below.
* `item_type [str]`: Identifies where the content field can be found. Can be one of `inline`, `storage`, `ipfs`.
* `item_content [Optional[str]]`: If `item_type == inline`, contains the JSON content of the message serialized as a string.

## Message types

Actual content sent by regular users can currently be of five types:

- [AGGREGATE](./aggregate.md): key-value storage.
- [FORGET](./forget.md): delete other messages.
- [POST](./post.md): unique data posts (unique data points, events).
- [PROGRAM](./program.md): create and update programs (ex: lambda functions).
- [STORE](./store.md): store and update files.


## Item hash, type and content

Messages are uniquely identified by the `item_hash` field. 
This value is obtained by computing the hash of the `content` field. 
Currently, the hash can be obtained in one of two ways. 
If the content of the message is stored on IPFS, the `item_hash` of the message will be the CIDv0 of this content. 
Otherwise, if the message is stored on Aleph native storage or is included in the message, the item hash will be 
the SHA256 hash of the message in hexadecimal encoding. 
In the first case, the item type will be set to `ipfs`. 
In the second case, the item type will either be `inline` if the content is included in the message (serialized as a
string in the `item_content` field) or `storage`. 
Inline storage will be used for content up to 200kB. Beyond this size, users must upload the content as a file 
on an Aleph.im node prior to uploading the message.

## Signature

Aleph.im messages are cryptographically signed with the private key of the user. 
The signature covers the `sender`, `chain`, `type` and `item_hash` fields of the message.
