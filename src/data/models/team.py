from dataclasses import dataclass, fields
import numpy as np


@dataclass(init=True)
class Team:
    bp_id: str
    city: str
    abbreviation: str
    name: str
    record: dict  # Dict[Literal['W','L','T'], int]

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, field.type(value))

    def _check_record_keys(self):
        return ({'W', 'L'} - set(self.record.keys())) == set()

    @property
    def games_played(self):
        return self.record['W'] + self.record['L'] if self._check_record_keys() else 0

    @property
    def win_percentage(self):
        return self.record['W'] / (self.games_played) if (self._check_record_keys() and self.games_played) else np.nan

    def to_dict(self):
        return self.__dict__ | {'win_percentage': self.win_percentage}
