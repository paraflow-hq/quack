#!/usr/bin/env python3

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        validate_default=True,  # 即使有默认值，我们也希望 before、after 验证生效
    )
