from dataclasses import dataclass, fields
from typing import Optional
from logs import logger_bp as logger


@dataclass(init=True)
class Player:
    bp_id: int  # -1 if not found
    first_name: str
    last_name: str
    position: str
    team: str
    conference: str
    division: str

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, field.type(value))

    def to_dict(self):
        return self.__dict__
