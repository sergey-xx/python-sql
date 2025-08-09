from dataclasses import dataclass, fields
from datetime import datetime
from typing import Self

import pymysql.cursors

from configs import (DB_HOST, DB_PORT, MYSQL_DATABASE, MYSQL_ROOT_PASSWORD,
                     MYSQL_USER)
from models import Room, Student


@dataclass
class DatabaseModel:

    __tablename__: str
    __indexes__: list | None = None

    @classmethod
    def connection(cls):
        return pymysql.Connection(
            host=DB_HOST,
            user=MYSQL_USER,
            password=MYSQL_ROOT_PASSWORD,
            port=int(DB_PORT),
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )

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
        with cls.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, args)
                result = cursor.fetchall()
            connection.commit()
            return result


class StudentDB(Student, DatabaseModel):

    __tablename__ = 'students'
    __indexes__ = ['room']

    @classmethod
    def get_foreign_key_sql(cls):
        return ', FOREIGN KEY (room) REFERENCES rooms(id)'

    def insert(self):
        sql = (
            f'INSERT INTO `{self.__tablename__}` '
            '(`id`, `name`, `room`, `birthday`, `sex`) '
            'VALUES (%s, %s, %s, %s, %s)')
        self.execute(
            sql, (self.id, self.name, self.room, self.birthday, self.sex)
        )

    @classmethod
    def isert_many(cls, objects: list[Self]):
        if not objects:
            return
        sql = (
            f'INSERT INTO `{cls.__tablename__}` '
            '(`id`, `name`, `room`, `birthday`, `sex`) '
            'VALUES (%s, %s, %s, %s, %s)')
        data = [
            (obj.id, obj.name, obj.room, obj.birthday, obj.sex)
            for obj in objects
        ]
        with cls.connection() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, data)
            connection.commit()


class RoomDB(Room, DatabaseModel):

    __tablename__ = 'rooms'

    def insert(self):
        sql = (
            f'INSERT INTO `{self.__tablename__}` '
            '(`id`, `name`) VALUES (%s, %s)'
        )
        self.execute(sql, (self.id, self.name))

    @classmethod
    def isert_many(cls, objects: list[Self]):
        if not objects:
            return
        sql = (
            f'INSERT INTO `{cls.__tablename__}` '
            '(`id`, `name`) VALUES (%s, %s)'
        )
        data = [(obj.id, obj.name) for obj in objects]
        with cls.connection() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, data)
            connection.commit()

    @classmethod
    def get_student_num_list(cls):
        """List of rooms and the number of students in each."""
        sql = '''
        SELECT rooms.*, COUNT(students.id) AS students_amount
        FROM rooms
        JOIN students ON students.room=rooms.id
        GROUP BY rooms.id
        '''
        res = cls.execute(sql)
        rooms = []
        for r in res:
            students_amount = r.pop('students_amount')
            rooms.append((cls(**r), students_amount))
        return [cls(**r) for r in res]

    @classmethod
    def get_top_5_rooms_with_smallest_average_student_age(cls):
        """Top 5 rooms with the smallest average student age."""
        sql = '''
        SELECT rooms.*,
               AVG(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) AS awg_age
        FROM rooms
        JOIN students ON students.room=rooms.id
        GROUP BY rooms.id
        ORDER BY awg_age
        LIMIT 5
        '''
        res = cls.execute(sql)
        return [cls(**r) for r in res if r.pop('awg_age')]

    @classmethod
    def get_top_5_rooms_with_largest_age_difference_among_students(cls):
        """Top 5 rooms with the largest age difference among students."""
        sql = '''
        SELECT rooms.*,
               TIMESTAMPDIFF(YEAR, MIN(birthday), MAX(birthday)) AS age_dif
        FROM rooms
        JOIN students ON students.room=rooms.id
        GROUP BY rooms.id
        ORDER BY age_dif DESC
        LIMIT 5
        '''
        res = cls.execute(sql)
        return [cls(**r) for r in res if r.pop('age_dif')]

    @classmethod
    def get_list_of_rooms_with_different_sexes(cls):
        """ List of rooms where students of different sexes live together."""
        sql = '''
        SELECT rooms.*
        FROM rooms
        JOIN students ON students.room=rooms.id
        GROUP BY rooms.id
        HAVING COUNT(DISTINCT students.sex) > 1;
        '''
        res = cls.execute(sql)
        return [cls(**r) for r in res]
