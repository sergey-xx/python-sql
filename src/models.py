from dataclasses import asdict, dataclass
from abc import ABC
from datetime import datetime


@dataclass
class AbstractModel(ABC):

    id: int
    name: str

    @classmethod
    def from_dict(cls, dct):
        return cls(**dct)

    @classmethod
    def from_list(cls, lst):
        return [cls.from_dict(dct) for dct in lst]

    def to_dict(self):
        return asdict(self)


@dataclass
class Room(AbstractModel):

    ...


@dataclass
class Student(AbstractModel):

    room: int
    birthday: datetime
    sex: str

    def __init__(self, *args, room, birthday, sex, **kwargs):
        self.room = room
        self.birthday = datetime.fromisoformat(birthday)
        self.sex = sex
        super().__init__(*args, **kwargs)
