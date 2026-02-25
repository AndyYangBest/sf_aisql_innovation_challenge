from __future__ import annotations

import warnings

warnings.filterwarnings(
    "ignore",
    message=r'Field name "schema" in ".*" shadows an attribute in parent "BaseModel"',
    category=UserWarning,
    module=r"pydantic\._internal\._fields",
)
