from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll, VerticalGroup, Vertical
from textual.widgets import Button, Static, Header, Footer, DirectoryTree, Tree, Input
from textual import on

from textual.widgets import Header, Select

class TreeApp(VerticalGroup):
    def compose(self) -> ComposeResult:
        tree: Tree[str] = Tree("Dune")
        tree.root.expand()

        for root in ["Characters", "Planets", "Stars"]:
            node = tree.root.add(root, expand=True)
            node.add_leaf("name: Paul")
            node.add_leaf("name: Jessica")
            node.add_leaf("name: Chani")

        tree.styles.width = "40%"

        yield tree

class SelectApp(VerticalGroup):
    CSS_PATH = "css/select"

    def __init__(self, selections, label):
        super().__init__()
        self.selections = selections
        self.label = label

    def compose(self) -> ComposeResult:
        yield Static(self.label)
        yield Select(self.selections, name="Antenna Array")

    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        self.title = str(event.value)


class DirectoryTreeApp(VerticalGroup):
    def compose(self) -> ComposeResult:
        yield DirectoryTree("./")


class ExitButton(VerticalGroup):
    # CSS_PATH = "css/button.tcss"

    def compose(self) -> ComposeResult:
        yield Button("Exit", variant="primary")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.app.exit()

class UploadApp(App):
    BINDINGS = [("d", "toggle_dark", "Toggle Dark Mode")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
        yield Horizontal(
            Vertical(
            TreeApp(),
            ),
            Vertical(
                Input(placeholder="filename", type="text"),
                SelectApp(
                    label="Antenna Array",
                    selections=(
                        ("alma", "ALMA"),
                        ("vla", "VLA"),
                        ("single-dish", "SINGLE-DISH")
                    )
                ),
                SelectApp(
                    label="Observation Type",
                    selections=(
                        ("interferometry", "interferometry"),
                        ("holography", "holography"),
                        ("single-dish", "single-dish")
                    )
                ),
                SelectApp(
                    label="Data Format",
                    selections=(
                        ("MSv4", "MSv4"),
                        ("MSv2", "MSv2"),
                        ("zarr", "zarr"),
                        ("asdm", "asdm"),
                        ("other", "other"),
                    )
                ),
                ExitButton()
            )
        )

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )


def app_test():
    app = UploadApp()
    print(app.run())