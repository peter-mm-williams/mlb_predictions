from dataclasses import dataclass, fields

@dataclass(init=True)
class Weather:
    temperature: float
    wind_speed: float
    wind_direction: str
    wind_degree: float
    humidity: float
    pressure: float
    precipitation_chance: float
    
    def __post_init__(self):
        for field in fields(self):        
            value = getattr(self, field.name)
            setattr(self, field.name, field.type(value))
    
    def to_dict(self):
        return self.__dict__