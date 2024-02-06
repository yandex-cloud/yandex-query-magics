from typing import Dict


def apply_template(sql: str, user_ns: Dict[str, object]) -> str:
    try:
        from jinja2 import Template
    except Exception as e:
        raise ValueError(
            "Jinja2 must be installed to use --jinja2: %pip3 install Jinja2"
        ) from e

    t = Template(sql)
    return t.render(user_ns)
