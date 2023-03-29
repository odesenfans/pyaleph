# Metrics

## Introduction

To facilitate your monitoring effort, pyaleph expose some metrics on the
server health and synchronisation status.

These metrics are available in two formats: JSON at
`/metrics.json` on the web port or in the [Prometheus
format](https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md)
at the standard [/metrics].

### Description

Below are the current metrics, a lot more are coming in the future as we
improve this interface.

| Field                                           | Type           | Description                                                                                      |
|-------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------|
| pyaleph_build_info                              | dict           | Software build information.                                                                      |
| pyaleph_status_peers_total                      | int            | Total number of peers of this node on the Aleph.im network.                                      |
| pyaleph_status_sync_messages_total              | int            | Number of messages processed by this node.                                                       |
| pyaleph_status_sync_permanent_files_total       | int            | Total number of permanent files stored.                                                          |
| pyaleph_status_sync_pending_messages_total      | int            | Number of messages received by the node and queued for processing.                               |
| pyaleph_status_sync_pending_txs_total           | int            | Number of on-chain transactions that are queued for processing.                                  |
| pyaleph_status_sync_messages_reference_total    | Optional[int]  | Number of processed messages on the reference node. (see aleph.reference_node_url config option) |
| pyaleph_status_sync_messages_remaining_total    | Optional[int]  | Message difference with the reference node.                                                      |
| pyaleph_status_chain_eth_last_committed_height  | Optional[int]  | Last treated block height in the ETH chain.                                                      |
| pyaleph_status_chain_eth_height_reference_total | Optional[int]  | Last known block height on the ETH chain.                                                        |
| pyaleph_status_chain_eth_height_remaining_total | Optional[int]  | Remaining blocks to process on the ETH chain.                                                    |

## Use with prometheus

To use with prometheus simply add <your server url>:4024/metrics as a
target in your prometheus.py. Eg prometheus file.

```yaml
global:  
  scrape_interval:     5s
  evaluation_interval: 5s

scrape_configs:
- job_name: aleph_demo
  static_configs:
    - targets: ['pyaleph:4024']
```

Make sure your http port is accessible from the prometheus server.
