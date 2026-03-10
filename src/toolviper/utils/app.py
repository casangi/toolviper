from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll, VerticalGroup
from textual.widgets import Button, Static, Header, Footer, DirectoryTree


class UploadApp(App):
    BINDINGS = [("d", "toggle_dark", "Toggle Dark Mode")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
        yield VerticalScroll(
            DirectoryTreeApp(),
            ExitButton(),
        )

    def action_toggle_dark(self) -> None:
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )


def app_test():
    app = UploadApp()
    print(app.run())


class DirectoryTreeApp(VerticalGroup):
    def compose(self) -> ComposeResult:
        yield DirectoryTree("./")


class ExitButton(VerticalGroup):
    # CSS_PATH = "css/button.tcss"

    def compose(self) -> ComposeResult:
        yield Button("Exit", variant="primary")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.app.exit()
