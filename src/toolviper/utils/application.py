import json
import toolviper

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Header,
    Footer,
    Static,
    Input,
    Select,
    Button,
    DirectoryTree,
    Label,
    DataTable,
    RichLog,
)
from textual.binding import Binding

import sys
import logging


class RichLogIO:
    """A file-like object that writes to a RichLog."""

    def __init__(self, log: RichLog):
        self.log = log

    def write(self, text: str) -> None:
        if text.strip():
            self.log.write(text.strip())

    def flush(self) -> None:
        pass


class RichLogHandler(logging.Handler):
    """A logging handler that writes to a RichLog."""

    def __init__(self, log: RichLog):
        super().__init__()
        self.log = log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.log.write(msg)

        except Exception:
            self.handleError(record)


class MetaDataBuilder(App):
    """A Textual app to build a JSON file from user inputs."""

    CSS = """
    Screen {
        background: $surface;
    }

    #left-pane {
        width: 30%;
        height: 100%;
        border-right: tall rgba(35, 83, 179, 0.8);
    }

    #right-pane {
        width: 70%;
        height: 100%;
        padding: 1;
    }

    #form-container {
        height: 1fr;
    }

    #log-container {
        height: 1fr;
        border-top: solid rgba(35, 83, 179, 0.8);
        margin-top: 1;
    }

    RichLog {
        height: 100%;
        background: $surface;
    }

    .input-field {
        margin-bottom: 0;
        border: solid rgba(35, 83, 179, 0.8);
    }

    #json-preview {
        height: 50%;
        border-top: solid rgba(35, 83, 179, 0.8);
        padding: 1;
        overflow-y: scroll;
    }

    #directory-tree {
        height: 50%;
    }

    #buttons-container {
        height: auto;
        align-horizontal: right;
        margin-top: 1;
    }

    #btn-add, #btn-done {
        margin-left: 1;
    }

    DataTable {
        height: 8;
        margin-top: 1;
        border: solid rgba(35, 83, 179, 0.8);
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", show=True),
        Binding("d", "toggle_dark", "Toggle Dark Mode"),
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.entries: list[dict[str, str]] = []
        self.dark = True
        self.selected_output_file: str = "output.json"

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="left-pane"):
                yield Label("Select Output File:")
                yield DirectoryTree("./", id="directory-tree")
                yield Label(
                    f"Output: {self.selected_output_file}", id="selected-file-label"
                )
                yield Static("JSON Preview", classes="label")
                yield Static("", id="json-preview")
            with Vertical(id="right-pane"):
                with Vertical(id="form-container"):
                    yield Label("Type:")
                    yield Select(
                        options=[
                            (opt, opt)
                            for opt in ["MSv2", "MSv4", "ZARR", "ASDM", "OTHER"]
                        ],
                        id="input-type",
                        classes="input-field",
                    )
                    yield Label("Telescope:")
                    yield Select(
                        options=[(opt, opt) for opt in ["VLA", "ALMA", "SINGLE-DISH"]],
                        id="input-telescope",
                        classes="input-field",
                    )
                    yield Label("Mode:")
                    yield Select(
                        options=[
                            (opt, opt)
                            for opt in ["HOLOGRAPHY", "INTERFEROMETRY", "SINGLE-DISH"]
                        ],
                        id="input-mode",
                        classes="input-field",
                    )
                    yield Label("Filename:")
                    yield Input(
                        placeholder="Enter filename...",
                        id="input-filename",
                        classes="input-field",
                    )

                    yield Label("Entries Added:")
                    yield DataTable(id="entries-table")

                with Vertical(id="log-container"):
                    yield Label("System Logs:")
                    yield RichLog(id="system-log", highlight=True, markup=True)

                with Horizontal(id="buttons-container"):
                    yield Button("Add Entry", variant="primary", id="btn-add")
                    yield Button("Write", variant="success", id="btn-done")
        yield Footer()

    def on_mount(self) -> None:
        # Redirect stdout and stderr to RichLog
        log_widget = self.query_one("#system-log", RichLog)
        self._stdout_orig = sys.stdout
        self._stderr_orig = sys.stderr
        sys.stdout = RichLogIO(log_widget)
        sys.stderr = RichLogIO(log_widget)

        table = self.query_one(DataTable)
        table.add_columns("Filename", "Type", "Telescope", "Mode")

        self.update_json_preview()

    def on_unmount(self) -> None:
        # Restore stdout and stderr
        sys.stdout = self._stdout_orig
        sys.stderr = self._stderr_orig

    def update_json_preview(self) -> None:
        preview = self.query_one("#json-preview", Static)
        try:
            preview.update(json.dumps(self.entries, indent=2))
        except Exception as e:
            preview.update(f"Error generating preview: {e}")

    def on_directory_tree_file_selected(
        self, event: DirectoryTree.FileSelected
    ) -> None:
        """Called when the user selects a file in the directory tree."""
        self.selected_output_file = str(event.path)
        label = self.query_one("#selected-file-label", Label)
        label.update(f"Output: {self.selected_output_file}")
        self.notify(f"Output file set to: {self.selected_output_file}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-add":
            self.action_add_entry()
        elif event.button.id == "btn-done":
            self.action_save_done()

    def action_add_entry(self) -> None:
        type_val = self.query_one("#input-type", Select).value
        telescope_val = self.query_one("#input-telescope", Select).value
        mode_val = self.query_one("#input-mode", Select).value
        filename_val = self.query_one("#input-filename", Input).value

        if not filename_val:
            self.notify("Filename is required!", severity="error")
            return

        if any(
            v in (Select.BLANK, Select.NULL)
            for v in [type_val, telescope_val, mode_val]
        ):
            self.notify("Please select all options!", severity="error")
            return

        entry = {
            "filename": filename_val,
            "type": str(type_val),
            "telescope": str(telescope_val),
            "mode": str(mode_val),
        }
        self.entries.append(entry)

        # Update table
        table = self.query_one(DataTable)
        table.add_row(filename_val, str(type_val), str(telescope_val), str(mode_val))

        self.update_json_preview()

        # Clear filename for the next entry
        self.query_one("#input-filename", Input).value = ""
        self.notify(f"Added entry: {filename_val}")

    def action_save_done(self) -> None:
        if not self.entries:
            self.notify("No entries to save!", severity="warning")
            return

        try:
            with open(self.selected_output_file, "w") as f:
                json.dump(self.entries, f, indent=2)

            self.notify(f"Saved to {self.selected_output_file}")

        except Exception as e:
            self.notify(f"Error saving file: {e}", severity="error")


if __name__ == "__main__":
    app = MetaDataBuilder()
    app.run()
