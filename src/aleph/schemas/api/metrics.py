from typing import Optional

from pydantic import BaseModel


class BuildInfo(BaseModel):
    """Dataclass used to export aleph node build info."""

    python_version: str
    version: str
    # branch: str
    # revision: str

class MetricsResponse(BaseModel):
    pyaleph_build_info: BuildInfo

    pyaleph_status_peers_total: int

    pyaleph_status_sync_messages_total: int
    pyaleph_status_sync_permanent_files_total: int

    pyaleph_status_sync_pending_messages_total: int
    pyaleph_status_sync_pending_txs_total: int

    pyaleph_status_chain_eth_last_committed_height: Optional[int]

    pyaleph_processing_pending_messages_seen_ids_total: Optional[int] = None
    pyaleph_processing_pending_messages_tasks_total: Optional[int] = None
    pyaleph_processing_pending_messages_aggregate_tasks: int = 0
    pyaleph_processing_pending_messages_forget_tasks: int = 0
    pyaleph_processing_pending_messages_post_tasks: int = 0
    pyaleph_processing_pending_messages_program_tasks: int = 0
    pyaleph_processing_pending_messages_store_tasks: int = 0

    pyaleph_processing_pending_messages_action_total: Optional[int] = None

    pyaleph_status_sync_messages_reference_total: Optional[int] = None
    pyaleph_status_sync_messages_remaining_total: Optional[int] = None
    pyaleph_status_chain_eth_height_reference_total: Optional[int] = None
    pyaleph_status_chain_eth_height_remaining_total: Optional[int] = None
