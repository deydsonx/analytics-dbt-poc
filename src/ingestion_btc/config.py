"""CLI args + Databricks secrets -> Config."""
import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    workspace_url: str
    zerobus_endpoint: str
    client_id: str
    client_secret: str
    catalog: str
    schema: str
    table: str
    ws_url: str
    topic: str

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def _get_secret(scope: str, key: str) -> str:
    from databricks.sdk.runtime import dbutils
    return dbutils.secrets.get(scope, key)


def parse() -> Config:
    p = argparse.ArgumentParser()
    p.add_argument("--workspace-url", required=True)
    p.add_argument("--zerobus-endpoint", required=True)
    p.add_argument("--secret-scope", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--ws-url", required=True)
    p.add_argument("--topic", required=True)
    a = p.parse_args()
    return Config(
        workspace_url=a.workspace_url,
        zerobus_endpoint=a.zerobus_endpoint,
        client_id=_get_secret(a.secret_scope, "zerobus-client-id"),
        client_secret=_get_secret(a.secret_scope, "zerobus-client-secret"),
        catalog=a.catalog,
        schema=a.schema,
        table=a.table,
        ws_url=a.ws_url,
        topic=a.topic,
    )
