from pathlib import Path

import pymysql

from configs import (DB_HOST, DB_PORT, MYSQL_DATABASE, MYSQL_ROOT_PASSWORD,
                     MYSQL_USER)
from db.config import MySQLConfig
from db.manager import DBManager
from db.models import RoomDB, StudentDB
from filehandler import JsonHandler

FILE_PATH = Path(__file__).resolve().parent.parent / 'files'

if __name__ == '__main__':
    connection_config = MySQLConfig(
        password=MYSQL_ROOT_PASSWORD,
        port=DB_PORT,
        database=MYSQL_DATABASE,
        host=DB_HOST,
        user=MYSQL_USER,
    )
    db_manager = DBManager(
        connection_class=pymysql.Connection,
        connection_config=connection_config,
    )
    RoomDB.create_table()
    StudentDB.create_table()
    fh = JsonHandler(FILE_PATH / 'rooms.json')
    rooms = RoomDB.from_list(fh.read_file())
    RoomDB.isert_many(rooms)
    fh = JsonHandler(FILE_PATH / 'students.json')
    students = StudentDB.from_list(fh.read_file())
    StudentDB.isert_many(students)
    for func in (
        RoomDB.get_student_num_list,
        RoomDB.get_top_5_rooms_with_smallest_average_student_age,
        RoomDB.get_top_5_rooms_with_largest_age_difference_among_students,
        RoomDB.get_list_of_rooms_with_different_sexes,
    ):
        print(func())

    for tablename in ('students', 'rooms'):
        sql = f'DROP TABLE IF EXISTS `{tablename}`'
        db_manager.execute(sql)
