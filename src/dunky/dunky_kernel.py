import subprocess
import duckdb
from ipykernel.kernelbase import Kernel
import os
import re
from tabulate import tabulate



def display_data(header: str, rows: list):
    """Generate the display data for the Jupyter frontend"""
    d = {
        "data": {
            "text/latex": tabulate(rows, header, tablefmt="latex_booktabs"),
            "text/plain": tabulate(rows, header, tablefmt="simple"),
            "text/html": tabulate(rows, header, tablefmt="html"),
        },
        "metadata": {},
    }
    return d


def is_select_query(query: str):
    select_query_pattern = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)
    return select_query_pattern.match(query)


def is_show_query(query: str):
    show_query_pattern = re.compile(r"^\s*SHOW\s+", re.IGNORECASE)
    return show_query_pattern.match(query)


def is_attach_query(query: str):
    attach_query_pattern = re.compile(r"^\s*ATTACH\s+", re.IGNORECASE)
    return attach_query_pattern.match(query)


def is_detach_query(query: str):
    detach_query_pattern = re.compile(r"^\s*DETACH\s+", re.IGNORECASE)
    return detach_query_pattern.match(query)


def is_create_external_table_as_select_query(query: str) -> bool:
    pattern = re.compile(
        r"^\s*CREATE\s+EXTERNAL\s+TABLE\s+.*\s+AS\s+SELECT\s+", re.IGNORECASE
    )
    return bool(pattern.search(query))


class DunkyKernel(Kernel):
    implementation = "Dunky"
    implementation_version = "1.0"
    language = "sql"
    language_version = "1.0"
    language_info = {
        "name": "DuckDBSQL",
        "codemirror_mode": "sql",
        "mimetype": "text/x-sql",
        "file_extension": ".sql",
    }
    banner = (
        "Dunky DuckDB with Unity Catalog kernel - run SQL queries directly in Jupyter"
    )

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        # Catch KeyboardInterrupt, cancel query, raise QueryCancelledError
        self._conn = duckdb.connect(":memory:")  # nothing is persisted to disk
        self._bootstrap()  # install uc_catalog, delta, load delta, load uc_catalog, create secret

    def _bootstrap(self):
        self._conn.sql(f"""
        INSTALL uc_catalog from core_nightly;
        INSTALL delta from core;
        LOAD delta;
        LOAD uc_catalog;
        CREATE SECRET (
            TYPE UC,
            TOKEN '{os.environ.get('UC_TOKEN', 'not-used')}',
            ENDPOINT '{os.environ.get('UC_ENDPOINT', "http://localhost:8080")}',
            AWS_REGION '{os.environ.get('UC_AWS_REGION', "eu-west-1")}'
        );
        """)

    def is_create_external_table_as_select_query(query: str) -> bool:
        pattern = re.compile(
            r"^\s*CREATE\s+EXTERNAL\s+TABLE\s+.*\s+AS\s+SELECT\s+", re.IGNORECASE
        )
        return bool(pattern.search(query))

    def _run_select_query(self, query: str, silent: bool):
        """Create a pandas dataframe from the result of a select query
        and generate the display data for the Jupyter frontend"""
        df = self._conn.sql(query).df()  # to pandas df
        header = df.columns.tolist()
        rows = df.values.tolist()
        output = display_data(header, rows)
        if not silent:
            self.send_response(self.iopub_socket, "display_data", output)

    def _run_show_query(self, query: str, silent: bool):
        """Run a show query"""
        result = self._conn.sql(query)
        if isinstance(result, duckdb.DuckDBPyRelation):
            df = result.df()
            header = df.columns.tolist()
            rows = df.values.tolist()
            if len(rows) == 0:
                output = {
                    "data": {
                        "text/plain": "No results found.",
                    },
                    "metadata": {},
                }
            else:
                output = display_data(header, rows)
            if not silent:
                self.send_response(self.iopub_socket, "display_data", output)
            return

        output = str(result)
        if not silent:
            self.send_response(self.iopub_socket, "display_data", output)

    def _run_attach_query(self, query: str, silent: bool):
        """Run an attach query and set the attached database name if result is None"""
        result = self._conn.sql(query)
        if result is None:
            match = re.search(
                r"ATTACH\s+['\"]?(\w+)['\"]?\s+AS\s+(\w+)", query, re.IGNORECASE
            )
            if match:
                attached_db_name = match.group(1)
                output = f"Database '{attached_db_name}' attached successfully."
            else:
                output = "ATTACH query executed successfully."
        else:
            output = str(result)

        if not silent:
            self.send_response(
                self.iopub_socket,
                "display_data",
                {"data": {"text/plain": output}, "metadata": {}},
            )

    def _run_detach_query(self, query: str, silent: bool):
        """Run a detach query and set the detached database name if result is None"""
        result = self._conn.sql(query)
        if result is None:
            detached_db_name = re.search(
                r'DETACH\s+DATABASE\s+["\']?(\w+)["\']?', query, re.IGNORECASE
            ).group(1)
            output = f"Database '{detached_db_name}' detached successfully."
        else:
            output = str(result)

        if not silent:
            self.send_response(
                self.iopub_socket,
                "display_data",
                {"data": {"text/plain": output}, "metadata": {}},
            )

    def _run_shell_command(self, command: str, silent: bool):
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output = {
            "data": {
                "text/plain": result.stdout,
            },
            "metadata": {},
        }
        if result.stderr:
            output["data"]["text/plain"] += "\n" + result.stderr

        if not silent:
            self.send_response(self.iopub_socket, "display_data", output)

    def _run_create_external_table_as_select_query(self, query: str, silent: bool):
        from dunky.store import (
            store,
        )  # import here to enable pyarrow and unitycatalog extra
        from dunky.threading import run_with_timeout
        from dunky.config import DunkyTargetConfig

        target_config = DunkyTargetConfig.from_query(query)
        select_query_match = re.search(r"AS\s+(.*)", query, re.IGNORECASE | re.DOTALL)
        if not select_query_match:
            raise ValueError("Invalid CREATE EXTERNAL TABLE AS SELECT query")

        select_query = select_query_match.group(1).strip()
        df = self._conn.sql(select_query).arrow()

        run_with_timeout(
            func=store,
            args=(target_config, df),
            timeout=10,
        )
        catalog_name = target_config.table_config.catalog_name
        schema_name = target_config.table_config.schema_name
        table_name = target_config.table_config.table_name

        full_path = (
            f"{catalog_name}.{schema_name}.{table_name}"
        )
        # check if the table was created
        # first we need to detach and reattach the database
        self._conn.sql(f"DETACH DATABASE {catalog_name};")
        self._conn.sql(
            f"ATTACH '{catalog_name}' AS {catalog_name} (TYPE UC_CATALOG);"
        )
        # then we can check if the table exists
        try:
            self._conn.sql(f"SELECT * FROM {full_path} LIMIT 1;")
        except Exception:
            raise ValueError(
                f"External table '{catalog_name}.{schema_name}.{table_name}' not created successfully"
            )

        output = {
            "data": {
                "text/plain": f"External table '{full_path}' created successfully.",
            },
            "metadata": {},
        }

        if not silent:
            self.send_response(self.iopub_socket, "display_data", output)

    def _run_unknown_query_type(self, query: str, silent: bool):
        """Run a unknown type of query"""
        result = self._conn.sql(query)
        if not result:
            result = "Query executed successfully"
        output = {
            "data": {
                "text/plain": result,
            },
            "metadata": {},
        }

        if not silent:
            self.send_response(self.iopub_socket, "display_data", output)

    def do_execute(
        self,
        code,
        silent,
        store_history=True,
        user_expressions=None,
        allow_stdin=False,
        **kwargs,
    ):
        try:
            if code.startswith("!"):
                self._run_shell_command(code[1:], silent)
            elif is_select_query(code):
                self._run_select_query(code, silent)
            elif is_show_query(code):
                self._run_show_query(code, silent)
            elif is_attach_query(code):
                self._run_attach_query(code, silent)
            elif is_detach_query(code):
                self._run_detach_query(code, silent)
            elif is_create_external_table_as_select_query(code):
                self._run_create_external_table_as_select_query(code, silent)
            else:
                self._run_unknown_query_type(code, silent)
        except Exception as e:
            output = str(e)
            self.send_response(
                self.iopub_socket, "stream", {"name": "stderr", "text": output}
            )
            return {
                "status": "error",
                "execution_count": self.execution_count,
                "ename": "ProgrammingError",
                "evalue": str(e),
                "traceback": [],
            }
        else:
            return {
                "status": "ok",
                "execution_count": self.execution_count,
                "payload": [],
                "user_expressions": {},
            }


if __name__ == "__main__":
    from ipykernel.kernelapp import IPKernelApp

    IPKernelApp.launch_instance(kernel_class=DunkyKernel)
