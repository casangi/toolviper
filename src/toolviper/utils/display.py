import re
import operator
from IPython.core.display import HTML


def dict_to_html(d, indent=0):
    print(f"THIS FUNCTION WILL BE DEPRECATED SOON")

    html = ""
    for key, value in d.items():
        if isinstance(value, dict):
            html += f"<div style='margin-left: {indent}em;'><details><summary>{key}</summary>{dict_to_html(value, indent + 1)}</details></div>"
        else:
            html += f"<div style='margin-left: {indent}em;'><font color='blue'><strong>{key}:</strong></font> {value}</div>"

    return html


class DataDict(dict):
    def __init__(self, dictionary):
        super().__init__()
        self._dict = dictionary

    def __repr__(self, *args, **kwargs):
        return f"<class '{self.__class__.__name__}'>"

    @classmethod
    def from_dict(cls, dictionary):
        if isinstance(dictionary, dict):
            return cls(dictionary)

        return None

    @property
    def data(self):
        return self._dict

    def select(self, keys, in_place=False):
        _result = list(operator.itemgetter(*keys)(self._dict))

        if in_place:
            self._dict = _result

        return DataDict.from_dict({key: value for key, value in zip(keys, _result)})

    def get_entries_(self, keys):
        return {key: value for key, value in self._dict.items() if key in keys}

    def filter(self, query, in_place=False):
        _result = None

        if isinstance(query, list):
            _result = self.get_entries_(query)

        if isinstance(query, str):
            _result = {
                key: value for key, value in self._dict.items() if re.search(query, key)
            }

        if in_place:
            self._dict = _result
            return None

        return DataDict.from_dict(_result)

    def display(self, interactive=True):
        import rich
        from toolviper.utils.parameter import is_notebook

        if is_notebook() and interactive:
            from IPython.display import JSON

            return JSON(self._dict)

        return rich.print_json(data=self._dict)

    def html(self, indent=0):
        html = ""
        for key, value in self._dict.items():
            if isinstance(value, dict):
                html += f"<div style='margin-left: {indent}em;'><details><summary>{key}</summary>{dict_to_html(value, indent + 1)}</details></div>"

            else:
                html += f"<div style='margin-left: {indent}em;'><font color='blue'><strong>{key}:</strong></font> {value}</div>"

        return HTML(html)
