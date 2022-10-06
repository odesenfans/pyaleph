from typing import Dict, Iterable, List

import aiohttp
import pytest
from aleph_message.models import MessageType, ItemHash

from aleph.toolkit.split import split_iterable
from .utils import get_messages_by_keys
import pytest_asyncio

POSTS_URI = "/api/v0/posts.json"


def group_messages_by_ref(messages: Iterable[Dict]) -> Dict[ItemHash, List[Dict]]:
    messages_by_ref = {}

    originals, amends = split_iterable(
        messages, cond=lambda msg: msg.get("ref") is None
    )
    for original in originals:
        item_hash = ItemHash(original["item_hash"])
        post_amends = sorted(
            [amend for amend in amends if amend["content"]["ref"] == item_hash],
            key=lambda msg: msg["time"],
        )
        messages_by_ref[item_hash] = [original] + post_amends
    return messages_by_ref


@pytest_asyncio.fixture()
async def fixture_posts(fixture_post_messages):
    def merge_content(_messages: List[Dict]) -> Dict:
        original = _messages[0]
        for message in _messages[1:]:
            original["content"].update(message["content"])
        return original

    messages_by_ref = group_messages_by_ref(fixture_post_messages)
    return [merge_content(message_list) for message_list in messages_by_ref.values()]


def assert_posts_equal(posts: Iterable[Dict], expected_messages: Iterable[Dict]):
    posts_by_hash = {post["item_hash"]: post for post in posts}

    for expected_message in expected_messages:
        post = posts_by_hash[expected_message["item_hash"]]
        assert "_id" not in post

        assert post["chain"] == expected_message["chain"]
        assert post["channel"] == expected_message["channel"]
        assert post["sender"] == expected_message["sender"]
        assert post["signature"] == expected_message["signature"]

        if expected_message.get("forgotten_by", []):
            assert post["content"] is None
            continue

        if "content" not in expected_message["content"]:
            # TODO: there is a problem with the spec of posts: they can be specified
            #       without an internal "content" field, which does not break the
            #       endpoint but returns the content of message["content"] instead.
            #       We skip the issue for now.
            continue

        assert post["content"] == expected_message["content"]["content"]


async def get_posts(api_client, **params) -> aiohttp.ClientResponse:
    return await api_client.get(POSTS_URI, params=params)


async def get_posts_expect_success(api_client, **params):
    response = await get_posts(api_client, **params)
    assert response.status == 200, await response.text()
    data = await response.json()
    return data["posts"]


@pytest.mark.asyncio
async def test_get_posts(fixture_posts, ccn_api_client):
    # The POST messages in the fixtures file do not amend one another, so we should have
    # 1 POST = 1 message.
    posts = await get_posts_expect_success(ccn_api_client)

    assert_posts_equal(posts, fixture_posts)
