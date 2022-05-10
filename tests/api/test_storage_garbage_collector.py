"""
Tests for the storage module to check that temporary files are properly marked for deletion.
"""


import pytest
from aleph.web import create_app
from aiohttp import FormData
from io import StringIO


@pytest.mark.asyncio
async def test_store_temporary_file(mock_config, test_db, aiohttp_client):
    app = create_app()
    app["config"] = mock_config
    client = await aiohttp_client(app)

    file_content = b"Some file I'd like to upload"

    data = FormData()
    data.add_field("file", file_content)

    response = await client.post(f"/api/v0/storage/add_file", data=data)
    assert response.status == 200, await response.text()

    data = await response.json()
    print(data)
