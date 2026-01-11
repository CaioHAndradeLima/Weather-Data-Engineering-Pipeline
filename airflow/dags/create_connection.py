import requests

AIRBYTE_URL = "http://host.docker.internal:8000/api/v1"

SOURCE_ID = "3e6d7c39-e4e3-41d5-8cdf-a322959a40cd"
DESTINATION_ID = "fca1a9cb-b0a6-4755-8143-537d0c51672c"
WORKSPACE_ID = "1c09d0f7-0652-420d-b99b-50b1d0622e52"

TABLES_TO_SYNC = [
    {"name": "customers", "namespace": "public"},
    {"name": "orders", "namespace": "public"},
    {"name": "employees", "namespace": "public"},
]


# --------------------------------------------------
# STEP 1 — Discover schema
# --------------------------------------------------
def discover_catalog():
    response = requests.post(
        f"{AIRBYTE_URL}/sources/discover_schema",
        json={"sourceId": SOURCE_ID},
    )
    response.raise_for_status()
    return response.json()["catalog"]


# --------------------------------------------------
# STEP 2 — Select desired streams
# --------------------------------------------------
def select_streams(catalog, tables):
    selected = []

    for stream in catalog["streams"]:
        name = stream["stream"]["name"]
        namespace = stream["stream"].get("namespace")

        for table in tables:
            if name == table["name"] and namespace == table["namespace"]:
                stream["config"] = {
                    "selected": True,
                    "syncMode": "incremental",
                    "destinationSyncMode": "append",
                    "cursorField": [],
                    "primaryKey": [],
                }
                selected.append(stream)

    return selected


# --------------------------------------------------
# STEP 3 — List & find connection
# --------------------------------------------------
def list_connections():
    response = requests.post(
        f"{AIRBYTE_URL}/connections/list",
        json={"workspaceId": WORKSPACE_ID},
    )
    response.raise_for_status()
    return response.json()["connections"]


def find_connection(connections):
    for c in connections:
        if c["sourceId"] == SOURCE_ID and c["destinationId"] == DESTINATION_ID:
            return c
    return None


# --------------------------------------------------
# STEP 4 — Get full connection
# --------------------------------------------------
def get_connection(connection_id):
    response = requests.post(
        f"{AIRBYTE_URL}/connections/get",
        json={"connectionId": connection_id},
    )
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# STEP 5 — Merge streams (additive-only)
# --------------------------------------------------
def merge_streams(existing_streams, desired_streams):
    merged = {}

    # Index existing streams
    for s in existing_streams:
        key = (s["stream"]["name"], s["stream"].get("namespace"))
        merged[key] = s

    # Add new desired streams
    for s in desired_streams:
        key = (s["stream"]["name"], s["stream"].get("namespace"))
        if key not in merged:
            merged[key] = s

    return list(merged.values())


# --------------------------------------------------
# STEP 6 — Create / Update connection
# --------------------------------------------------
def create_connection(streams):
    payload = {
        "name": "postgres_to_snowflake_bronze",
        "sourceId": SOURCE_ID,
        "destinationId": DESTINATION_ID,
        "workspaceId": WORKSPACE_ID,
        "syncCatalog": {"streams": streams},
        "status": "active",
    }

    response = requests.post(
        f"{AIRBYTE_URL}/connections/create",
        json=payload,
    )
    response.raise_for_status()

    return response.json()["connectionId"]


def update_connection(connection_id, streams):
    payload = {
        "connectionId": connection_id,
        "syncCatalog": {"streams": streams},
    }

    response = requests.post(
        f"{AIRBYTE_URL}/connections/update",
        json=payload,
    )
    response.raise_for_status()


# --------------------------------------------------
# STEP 7 — Optional sync trigger
# --------------------------------------------------
def trigger_sync(connection_id):
    response = requests.post(
        f"{AIRBYTE_URL}/connections/sync",
        json={"connectionId": connection_id},
    )
    response.raise_for_status()


# --------------------------------------------------
# MAIN ENTRYPOINT (Airflow task)
# --------------------------------------------------
def run():
    connections = list_connections()
    connection = find_connection(connections)

    catalog = discover_catalog()
    desired_streams = select_streams(catalog, TABLES_TO_SYNC)

    if not desired_streams:
        raise RuntimeError("No matching tables found in source catalog")

    if not connection:
        print("No connection found. Creating new connection.")
        connection_id = create_connection(desired_streams)
    else:
        print("Connection exists. Reconciling tables.")
        connection_id = connection["connectionId"]

        existing = get_connection(connection_id)
        merged_streams = merge_streams(
            existing["syncCatalog"]["streams"],
            desired_streams,
        )

        update_connection(connection_id, merged_streams)

    print(f"Connection ready: {connection_id}")
    trigger_sync(connection_id)
    print("Sync triggered")
