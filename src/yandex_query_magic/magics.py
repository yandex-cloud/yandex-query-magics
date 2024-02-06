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
from yandex_query_magic import jinja_template
from typing import Optional, Dict
from .ipythondisplay import IpythonDisplay
import nest_asyncio
import json
from .sqltext_parser import SqlParser

nest_asyncio.apply()


@magics_class
class YQMagics(Magics):
    "Main class for Jupyter magics interop"

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
                               as_dataframe: bool = True) -> None:

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
            print("Folder id is not specified. "
                  "Specify it with %yq_settings "
                  "--folder_id <folder_id> extension")
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
            [label_query_id,
             widgets.HBox(
                 [start_status, stop_status]),
             widgets.HBox(
                 [progress, processed_gb, abort_query_button]),
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

        async def abort_query_async(query_id: str) -> None:
            try:
                await yq.stop_query(folder_id, query_id)
            except Exception as ex:
                stop_status.value = str(ex)

        # Callback to stop the query
        def abort_query(b):
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
                result = result.to_dataframe()
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
    @argument("--folder-id", help="Yandex cloud folder id to run queries", type=str)  # noqa
    def yq_settings(self, line):
        args = parse_argstring(self.yq_settings, line)

        if args.vm_auth:
            YQMagics.SA_info = None
        # read file with SA credentials
        elif args.sa_file_auth is not None:
            sa_file = args.sa_file_auth.strip()
            with open(sa_file, "r") as sa_file:
                sa_info = sa_file.read()
            YQMagics.Sa_info = json.loads(sa_info)

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
            query = jinja_template.apply_template(query, user_ns)
        elif args.no_var_expansion:
            pass
        else:
            parser = SqlParser()
            query = parser.reformat(query, user_ns)

        loop = asyncio.get_event_loop()
        query_result = None
        query_result = loop.run_until_complete(
            self.yq_execute_query(args.folder_id,
                                  query, args.name,
                                  args.description,
                                  not args.raw_results))
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
