import re
import operator


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

    @classmethod
    def from_dict(cls, dictionary):
        if isinstance(dictionary, dict):
            return cls(dictionary)

        return None

    def fetch(self, keys):
        return list(operator.itemgetter(*keys)(self._dict))

    def get_entries_(self, keys):
        return {key: value for key, value in self._dict.items() if key in keys}

    def filter(self, query):
        if isinstance(query, list):
            _result = self.get_entries_(query)
            return DisplayDict.from_dict(_result)

        if isinstance(query, str):
            _result = {
                key: value for key, value in self._dict.items() if re.search(query, key)
            }
            return self.from_dict(_result)

        return None

    def display(self):
        import rich
        from toolviper.utils.parameter import is_notebook

        if is_notebook():
            from IPython.display import JSON

            return JSON(self._dict)

        return rich.print_json(data=self._dict)
