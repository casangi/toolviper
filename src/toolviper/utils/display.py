import rich

def dict_to_html(d, indent=0):

    html = ""
    for key, value in d.items():
        if isinstance(value, dict):
            html += f"<div style='margin-left: {indent}em;'><details><summary>{key}</summary>{dict_to_html(value, indent + 1)}</details></div>"
        else:
            html += f"<div style='margin-left: {indent}em;'><strong>{key}:</strong> {value}</div>"
    return html

class DisplayDict(dict):
    def __init__(self, dictionary):
        super().__init__()
        self._dict = dictionary

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def display(self):
        import rich
        from toolviper.utils.parameter import is_notebook

        if is_notebook():
            from IPython.display import JSON
            print("notebook")
            return JSON(self._dict)

        return rich.print_json(data=self._dict)