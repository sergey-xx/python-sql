from dataclasses import asdict, dataclass
from typing import Type

import pymysql.cursors


@dataclass
class ConnectionConfig:
    password: str
    port: int
    database: str
    host: str = 'localhost'
    user: str = 'user'

    def to_dict(self):
        return asdict(self)


@dataclass
class MySQLConfig(ConnectionConfig):
    cursorclass: Type = pymysql.cursors.DictCursor

    def to_dict(self):
        d = super().to_dict()
        d['cursorclass'] = self.cursorclass
        return d
