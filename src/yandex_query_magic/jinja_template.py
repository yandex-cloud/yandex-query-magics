from typing import Dict, Any, Optional
from .sqltext_parser import SqlParser


class JinjaTemplate:

    @staticmethod
    def apply_template(sql: str, user_ns: Dict[str, object]) -> str:
        try:
            from jinja2 import Template, Environment
        except Exception as e:
            raise ValueError(
                "Jinja2 must be installed to use --jinja2: %pip3 install Jinja2"
            ) from e

        def to_yq(value: Any, name: Optional[str] = None) -> Any:
            return SqlParser.render_type(value, name)

        env = Environment()
        env.filters["to_yq"] = to_yq

        t = env.from_string(sql)
        return t.render(user_ns)
