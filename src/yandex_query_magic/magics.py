import os

from .main import YandexQuery, YandexQueryException
import argparse
from IPython.core.magic_arguments import (argument,
                                          magic_arguments,
                                          parse_argstring)

from IPython.core.magic import (Magics, magics_class,
                                line_cell_magic, line_magic,
                                no_var_expand)
from IPython.display import display
import re
from jupyter_ui_poll import ui_events
import asyncio
import ipywidgets as widgets
from datetime import datetime
from .jinja_template import JinjaTemplate
from typing import Optional, Dict
from .ipythondisplay import IpythonDisplay
import nest_asyncio
import json
from .sqltext_parser import SqlParser

nest_asyncio.apply()


@magics_class
class YQMagics(Magics):
    """Main class for Jupyter magics interop"""

    DefaultFolderId = None  # default folder to be used between queries
    Sa_info = None  # authentication token to be used between queries

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)
        self.ipython_display = IpythonDisplay()

    # Executes query in YQ
    async def yq_execute_query(self,
                               folder_id: Optional[str],
                               query_text: str,
                               name: Optional[str] = None,
                               description: Optional[str] = None,
                               as_dataframe: bool = True,
                               all_results: bool = False) -> None:

        yq = YandexQuery()
        if YQMagics.Sa_info is not None:
            yq.set_service_account_key_auth(YQMagics.Sa_info)
        else:
            yq.set_vm_auth()

        variable = None

        # parses query <variable> << select ...
        # extracts <variable> to be assigned
        # after query execution to query results
        match = re.search(r'(\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*<<(.*)',
                          query_text,
                          re.MULTILINE | re.DOTALL)

        if match is not None\
                and len(match.groups()) == 2\
                and str.isidentifier(match[1]):
            variable = match[1]
            query_text = match[2]

        if folder_id is None:
            folder_id = YQMagics.DefaultFolderId

        if folder_id is None:
            try:
                folder_id = await YandexQuery().resolve_vm_folder_id()
            except:
                pass

        if folder_id is None:
            print("Folder id is not specified. "
                  "Specify it with %yq_settings "
                  "--folder-id <folder_id> extension")
            return

        # We use rich interaction with UI and async query execution
        # so we need to run asyncio loops when we need it and interrupt it
        # in required places
        loop = asyncio.get_event_loop()

        query_id = None
        label_query_id = widgets.HTML()

        now_str = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        # shows start status of query
        start_status = widgets.Label(f"Query started at {now_str}.")

        # shows final information about query
        stop_status = widgets.Label("")

        # shows is truncated status
        is_truncated_label = widgets.Label("")
        is_truncated_label.style.text_color = 'violet'

        # shows only first dataset information
        several_datasets_label = widgets.Label("")
        several_datasets_label.style.text_color = 'violet'
        several_datasets_label.layout.display = 'none'

        # aborts execution of the query
        abort_query_button = widgets.Button(description="Abort")

        # shows the progress of query execution
        # It is not the best control to show progress of unknown length
        # But ipywidgets library has not another control
        progress = widgets.IntProgress(
            value=0,
            min=0,
            max=100,
            bar_style='info',
            style={'bar_color': 'green'},
            orientation='horizontal'
        )

        # Shows progress of query execution
        processed_gb = widgets.Label("")  # 0 GB

        # Hidden field for query execution issues
        issues = widgets.Textarea()
        issues.layout = widgets.Layout(width='600px', height="300px")
        issues.layout.display = 'none'
        issues.style = dict(text_color='red')

        # UI control layout
        all_widgets = widgets.VBox(
            [widgets.HBox([label_query_id, is_truncated_label]),
             widgets.HBox(
                 [start_status, stop_status]),
             widgets.HBox(
                 [progress, processed_gb, abort_query_button]),
             several_datasets_label,
             issues])

        # Update status callback from start_execute_query method
        def update_status(new_status: str) -> None:
            progress.description = new_status

        # Update progress callback from start_execute_query method
        def update_progress(new_value: int, query_started_at: datetime) -> None: # noqa
            progress.value = new_value
            query_execution_time = datetime.now().replace(tzinfo=None) - query_started_at # noqa
            stop_status.value = f"Executing for {query_execution_time} "\
                                "seconds so far"

            # Singe we need to react on Abort button,
            # we need to process UI event loop from time to time
            with ui_events() as ui_poll:
                ui_poll(1)

        async def abort_query_async(query_id_: str) -> None:
            try:
                await yq.stop_query(folder_id, query_id_)
            except Exception as stop_ex:
                stop_status.value = str(stop_ex)

        # Callback to stop the query
        def abort_query(_):
            loop.create_task(abort_query_async(query_id))

        abort_query_button.on_click(abort_query)

        # Show all controls
        display(all_widgets)  # noqa

        started_at = datetime.now()
        query_id = await yq.start_execute_query(
            folder_id,
            query_text,
            name,
            description)

        label_query_id.value = f"Query id is <a style='text-decoration: underline;'"\
                               f" href='https://yq.cloud.yandex.ru/folders/{folder_id}/ide/queries/{query_id}'"\
                               f" target='_blank'>{query_id}</a>." # noqa

        result = None
        try:
            # Start query execution
            await yq.wait_results(
                    folder_id,
                    query_id,
                    update_status,
                    update_progress)

            try:
                query_info = await yq.get_queryinfo(
                                folder_id,
                                query_id)

                query_status = query_info["status"]
                progress.description = query_status

                for resultSet in query_info["result_sets"]:
                    is_truncated_rs = resultSet.get("truncated", False)
                    if is_truncated_rs:
                        is_truncated_label.value = "Results were truncated"

                progress.description = "Fetching results"

                # Retrieving query results
                try:
                    if query_status == "COMPLETED":
                        result = await yq.get_query_result(folder_id, query_id)
                        progress.description = "DONE"
                        progress.bar_style = "success"

                    else:
                        progress.value = 0
                        progress.description = query_status
                        progress.bar_style = "danger"

                        if "issues" in query_info:
                            issues.value = json.dumps(query_info["issues"])
                            issues.layout.display = 'block'

                # If YQ query execution happened, show details
                except YandexQueryException as ex:
                    issues.value = ex.issues.__repr__()
                    issues.layout.display = 'block'

            except Exception as ex:
                self.ipython_display.error(ex.__repr__())
                # issues.value = ex.__repr__()
                # issues.layout.display = 'block'

        finally:
            # Hide abort query button after query execution completed
            abort_query_button.layout.display = 'none'

        total_time = str(datetime.now()-started_at)
        finish_time_str = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        stop_status.value = f"Finished at {finish_time_str}."\
                            f" Total time is {total_time}"

        if result is not None:
            if as_dataframe:
                result = result.to_dataframes(None)
                if not all_results:
                    if isinstance(result, list):
                        if len(result) > 1:
                            several_datasets_label.value = f"{len(result)} result sets returned"
                            several_datasets_label.layout.display = 'block'

                        if len(result) >= 1:
                            result = result[0]
                else:
                    return result
        else:
            result = result.raw_results

        # Write results to external variable
        if variable is not None:
            self.shell.user_ns[variable] = result
            return result
        else:
            return result

    @no_var_expand
    @line_magic
    @magic_arguments()
    @argument("--sa-file-auth", help="Authenticate using path to Yandex Cloud static credentials file", type=str)  # noqa
    @argument("--vm-auth", help="Authenticate use VM credentials", action="store_true")  # noqa
    @argument("--env-auth", help="Authenticate using credentials from environment variable", type=str)  # noqa
    @argument("--folder-id", help="Yandex cloud folder id to run queries", type=str)  # noqa
    def yq_settings(self, line):
        args = parse_argstring(self.yq_settings, line)

        if args.vm_auth:
            loop = asyncio.get_event_loop()

            async def resolve_vm():
                await YandexQuery().resolve_vm_folder_id()

            try:
                loop.run_until_complete(resolve_vm)
            except:
                self.ipython_display.error(f"Cannot connect to VM cloud agent")
                return

            YQMagics.SA_info = None
        # read file with SA credentials
        elif args.sa_file_auth is not None:
            sa_file = args.sa_file_auth.strip()
            if not os.path.exists(sa_file) or not os.path.isfile(sa_file):
                self.ipython_display.error(f"File {args.sa_file_auth} is not found")
                return

            with open(sa_file, "r") as sa_file:
                sa_info = sa_file.read()
            YQMagics.Sa_info = json.loads(sa_info)
        elif args.env_auth is not None:
            env_secret = os.getenv(args.env_auth, None)
            if env_secret is None:
                self.ipython_display.error(f"No secret found in environment variable [{args.env_auth}]")
                return

            YQMagics.Sa_info = json.loads(env_secret)

        if args.folder_id is not None:
            YQMagics.DefaultFolderId = args.folder_id

    @no_var_expand
    @magic_arguments()
    @line_cell_magic("yq")
    @argument("--folder-id", help="Yandex cloud folder id to run queries", type=str)  # noqa
    @argument("--name", help="Query name", type=str)
    @argument("--description", help="Query description", type=str)
    @argument("-j", "--jinja2", help="Apply Jinja2 Template", action="store_true")  # noqa
    @argument("--no-var-expansion", help="Disable {{var}} evaluation", action="store_true")  # noqa
    @argument("--all-results", help="Return all results, not only first", action="store_true")  # noqa
    @argument("--raw-results", help="Return result as raw YQ response", action='store_true', default=False)  # noqa
    @argument("rest", nargs=argparse.REMAINDER)
    def execute(self, line: Optional[str] = None,
                cell: Optional[str] = None) -> None:

        cell = "" if cell is None else cell
        line = "" if line is None else line
        user_ns: Dict[str, object] = self.shell.user_ns

        args = parse_argstring(self.execute, line)

        rest = " ".join(args.rest)
        query = f"{rest}\n{cell}".strip()

        if args.jinja2:
            query = JinjaTemplate.apply_template(query, user_ns)
        elif args.no_var_expansion:
            pass
        else:
            parser = SqlParser()
            query = parser.reformat(query, user_ns)

        loop = asyncio.get_event_loop()

        query_result = loop.run_until_complete(
            self.yq_execute_query(args.folder_id,
                                  query, args.name,
                                  args.description,
                                  not args.raw_results,
                                  args.all_results))

        return query_result


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(YQMagics)


def unload_ipython_extension(ip):
    """Unload the extension in IPython."""
    if 'YandexQuery' in ip.magics_manager.registry:
        del ip.magics_manager.registry['YQMagics']
    if 'YQMagics' in ip.config:
        del ip.config['YQMagics']
