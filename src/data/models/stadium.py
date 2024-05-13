from dataclasses import dataclass, fields

@dataclass(init=True)
class Stadium:
    bp_id: str
    name: str
    city: str
    state: str
    stadium_type: str
    capacity: int
    surface: str
    
    def __post_init__(self):
        for field in fields(self):        
            value = getattr(self, field.name)
            setattr(self, field.name, field.type(value))

    def to_dict(self):
        return self.__dict__
