from dataclasses import dataclass, fields
from datetime import datetime

from .manager import DBManager


@dataclass
class DatabaseModel:

    __tablename__: str
    __indexes__: list[str | list | tuple] | None = None
    _db_manager_class = DBManager

    @classmethod
    def connection(cls):
        return cls._db_manager_class().get_connection()

    @staticmethod
    def get_field_type(klass):
        return {
            str: 'VARCHAR(255)',
            int: 'int',
            datetime: 'DATETIME',
        }[klass]

    @classmethod
    def get_tablename(cls):
        return cls.__tablename__ or cls.__name__.lower()

    @classmethod
    def get_columns(cls):
        return {
            field.name: cls.get_field_type(field.type)
            for field in fields(cls)
            if not field.name.startswith('_')
        }

    @classmethod
    def get_foreign_key_sql(cls):
        return ''

    @classmethod
    def get_create_sql(cls):
        columns = cls.get_columns()
        columns_sql = ", ".join(
            [f"`{name}` {sql_type}" for name, sql_type in columns.items()]
        )
        return (
            f'CREATE TABLE IF NOT EXISTS '
            f'`{cls.__tablename__}` ({columns_sql}, '
            f'PRIMARY KEY (`id`)'
            f'{cls.get_foreign_key_sql()});'
        )

    @classmethod
    def create_indexes(cls):
        if not cls.__indexes__:
            return
        for index in cls.__indexes__:
            if isinstance(index, (list, tuple)):
                index_name = f'idx_{cls.__tablename__}_{"".join(index)}'
                sql = (
                    f'CREATE INDEX {index_name} '
                    f'ON {cls.__tablename__}({", ".join(index)});'
                )
            else:
                sql = (
                    f'CREATE INDEX idx_{cls.__tablename__}_{index} '
                    f'ON {cls.__tablename__}({index});'
                )
            cls.execute(sql)

    @classmethod
    def create_table(cls):
        sql = cls.get_create_sql()
        cls.execute(sql)
        cls.create_indexes()

    @classmethod
    def execute(cls, sql, args=None):
        return cls._db_manager_class().execute(sql, args=None)
