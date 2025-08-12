from typing import Type

from .config import ConnectionConfig


class Singleton(type):
    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls
            ).__call__(*args, **kwargs)
        return cls._instances[cls]


class DBManager(metaclass=Singleton):

    def __init__(
            self,
            connection_config: ConnectionConfig | None = None,
            connection_class: Type | None = None
    ) -> None:
        self._connection_config = connection_config
        self._connection_class = connection_class

    def get_connection(self):
        if not all((self._connection_config, self._connection_class)):
            raise ValueError('Configurate connection first')
        return self._connection_class(
            **self._connection_config.to_dict()
        )

    def execute(self, sql: str, args=None):
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, args)
                result = cursor.fetchall()
            connection.commit()
            return result
