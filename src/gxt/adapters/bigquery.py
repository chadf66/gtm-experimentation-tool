"""Simple BigQuery adapter stub.

This is a lightweight stub to be expanded with real Google Cloud BigQuery client
integration later.
"""
from .base import WarehouseAdapter
from typing import Any, Dict, Iterable, Optional

# Optional import for Google Cloud BigQuery client. We import lazily in from_profile
# to avoid hard dependency at runtime for users who only want dry-run behavior.
try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None


class BigQueryAdapter(WarehouseAdapter):
    def __init__(self, project: str = None, dataset: str = None, client: Optional[object] = None):
        self.project = project
        self.dataset = dataset
        # Optional BigQuery client (google.cloud.bigquery.Client). If None, adapter falls back to printing SQL.
        self.client = client

    @classmethod
    def from_profile(cls, profile: dict):
        """Create a BigQueryAdapter from a profile output dict.

        Expected fields: project, credentials (path or env var reference), location
        """
        project = None
        dataset = None
        client = None
        if isinstance(profile, dict):
            project = profile.get("project")
            dataset = profile.get("dataset")
            # Attempt to build a BigQuery client if google-cloud-bigquery is available.
            if bigquery is not None:
                try:
                    # Profile may include a credentials path or environment var reference under 'credentials'
                    creds = profile.get("credentials")
                    if creds:
                        # If credentials is a path to a service account JSON, user can set GOOGLE_APPLICATION_CREDENTIALS
                        # or we could accept a direct path — for now rely on application default credentials.
                        pass
                    client = bigquery.Client(project=project) if project else bigquery.Client()
                except Exception:
                    client = None
        return cls(project=project, dataset=dataset, client=client)

    def execute(self, sql: str) -> Any:
        """Execute SQL using the BigQuery client if available, otherwise print SQL.

        Returns the client result (rows) when client is available or an empty list when falling back.
        """
        if getattr(self, "client", None) is not None:
            # Use the BigQuery client to run the query and return results
            job = self.client.query(sql)
            result = job.result()
            # Convert to list of dicts for convenience
            rows = [dict(row) for row in result]
            return rows
        # Fallback: print SQL and return empty list
        print("[bigquery] execute SQL:\n", sql)
        return []

    def insert_rows(self, table: str, rows: Iterable[Dict[str, Any]]):
        """Insert rows into a table using client if available, otherwise print summary."""
        rows_list = list(rows)
        if getattr(self, "client", None) is not None:
            # Use insert_rows_json for simplicity
            dataset_table = table
            try:
                errors = self.client.insert_rows_json(dataset_table, rows_list)
                if errors:
                    raise RuntimeError(f"BigQuery insert errors: {errors}")
                return None
            except Exception as e:
                raise
        print(f"[bigquery] insert into {table}: {len(rows_list)} rows")

    def ensure_table_exists(self, table: str, schema: Optional[Iterable[Dict[str, str]]] = None, location: Optional[str] = None):
        """Ensure the specified table exists in BigQuery. If it does not exist and a client is available,
        create it using the provided schema. Schema should be an iterable of dicts with keys 'name' and 'type',
        e.g. [{'name':'experiment_id','type':'STRING'}, ...]. If client is not available, print instructions.
        """
        if getattr(self, "client", None) is None:
            print(f"[bigquery] ensure_table_exists: client not configured. Please create table {table} manually with schema: {schema}")
            return None

        # Parse table identifier: could be `project.dataset.table` or `dataset.table`
        parts = table.replace('`','').split('.')
        project = None
        dataset_id = None
        table_id = None
        if len(parts) == 3:
            project, dataset_id, table_id = parts
        elif len(parts) == 2:
            dataset_id, table_id = parts
            project = self.project or (getattr(self.client, 'project', None) if getattr(self, 'client', None) else None)
        elif len(parts) == 1:
            table_id = parts[0]
            # prefer adapter dataset, then client's default project/dataset
            dataset_id = self.dataset or (getattr(self.client, 'project', None) if getattr(self, 'client', None) else None)
            project = self.project or (getattr(self.client, 'project', None) if getattr(self, 'client', None) else None)
            if dataset_id is None:
                raise ValueError(f"Could not resolve dataset for table identifier: {table}; set assignments_table to dataset.table or configure profile dataset")
        else:
            raise ValueError(f"Unsupported table identifier format: {table}")

        dataset_ref = self.client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
            # table exists
            return None
        except Exception:
            # create the table
            from google.cloud import bigquery as gbq

            schema_fields = []
            if schema:
                for col in schema:
                    name = col.get('name')
                    typ = col.get('type', 'STRING')
                    schema_fields.append(gbq.SchemaField(name, typ))
            else:
                # default schema: experiment_id STRING, generic unit STRING, variant STRING, assigned_at TIMESTAMP
                schema_fields = [
                    gbq.SchemaField('experiment_id', 'STRING'),
                    gbq.SchemaField('unit', 'STRING'),
                    gbq.SchemaField('variant', 'STRING'),
                    gbq.SchemaField('assigned_at', 'TIMESTAMP'),
                ]

            table_obj = gbq.Table(table_ref, schema=schema_fields)
            if location:
                table_obj.location = location
            created = self.client.create_table(table_obj)
            print(f"[bigquery] Created table: {created.full_table_id}")
            return created

    def upsert_from_select(self, target_table: str, src_select_sql: str, key_columns: Iterable[str], insert_columns: Optional[Iterable[str]] = None):
        """Generate and execute a BigQuery MERGE statement that upserts rows from a SELECT.

        Parameters:
        - target_table: fully-qualified table identifier (e.g. `dataset.table` or `project.dataset.table`)
        - src_select_sql: SELECT statement producing rows with the same columns as the target
        - key_columns: iterable of column names to use as the merge key (must exist in both sides)

        This method constructs a MERGE ... WHEN NOT MATCHED THEN INSERT ... statement that
        inserts only rows that do not already exist in the target. It does not attempt to
        update existing rows (we intentionally only insert new assignments).
        """
        # Build ON clause comparing key columns
        key_cols = list(key_columns)
        if not key_cols:
            raise ValueError("key_columns must be provided for upsert_from_select")

        on_clauses = [f"T.{c} = S.{c}" for c in key_cols]
        on_sql = " AND ".join(on_clauses)

        # Strip trailing semicolons from embedded select to avoid parser issues
        src_select_sql = src_select_sql.strip()
        if src_select_sql.endswith(";"):
            src_select_sql = src_select_sql[:-1]

        # Ensure target_table is fully-qualified: accept table, dataset.table, or project.dataset.table
        parts = target_table.replace('`', '').split('.')
        if len(parts) == 3:
            fq_target = f"`{parts[0]}.{parts[1]}.{parts[2]}`"
        elif len(parts) == 2:
            fq_target = f"`{parts[0]}.{parts[1]}`"
        else:
            # try to use adapter dataset and project
            dataset = self.dataset or (getattr(self.client, 'project', None) if getattr(self, 'client', None) else None)
            project = self.project or (getattr(self.client, 'project', None) if getattr(self, 'client', None) else None)
            if dataset and project:
                fq_target = f"`{project}.{dataset}.{target_table}`"
            elif dataset:
                fq_target = f"`{dataset}.{target_table}`"
            else:
                fq_target = target_table

        # Determine insert columns (caller may pass explicit insert_columns). Fall
        # back to sensible defaults if none provided.
        insert_cols = list(insert_columns) if insert_columns is not None else None
        if insert_cols is None:
            insert_cols = ["experiment_id", "unit", "variant", "assigned_at"]

        insert_cols_sql = ", ".join(insert_cols)
        insert_vals_sql = ", ".join([f"S.{c}" for c in insert_cols])

        merge_sql = (
            f"MERGE INTO {fq_target} T\n"
            f"USING (\n{src_select_sql}\n) S\n"
            f"ON {on_sql}\n"
            f"WHEN NOT MATCHED THEN\n"
            f"  INSERT ({insert_cols_sql})\n"
            f"  VALUES ({insert_vals_sql})\n"
        )

        return self.execute(merge_sql)

    def hash_bucket_sql(self, column: str, salt: str = "", precision: int = 1_000_000) -> str:
        """Return a BigQuery SQL expression computing a deterministic bucket in [0,1).

        Uses FARM_FINGERPRINT for speed and uniformity. Handles optional salt by
        concatenating with '::'.
        """
        col = column
        if salt:
            col = f"CONCAT(CAST({column} AS STRING),'::','{salt}')"

        # FARM_FINGERPRINT returns a signed 64-bit integer; use ABS to avoid negatives.
        # Mod by precision and normalize to [0,1).
        # Use MOD(ABS(FARM_FINGERPRINT(...)), precision) to avoid stray percent characters
        return f"MOD(ABS(FARM_FINGERPRINT({col})), {precision})/{precision}.0"

    def qualify_table(self, dataset: str, table: str) -> str:
        """Return a BigQuery-qualified identifier for dataset.table, with optional project.

        Examples:
            qualify_table('analytics','users') -> `project.analytics.users` or `analytics.users`
        """
        if self.project:
            return f"`{self.project}.{dataset}.{table}`"
        return f"`{dataset}.{table}`"
