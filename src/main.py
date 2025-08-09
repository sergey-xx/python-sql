from pathlib import Path

from db import RoomDB, StudentDB
from filehandler import JsonHandler

FILE_PATH = Path(__file__).resolve().parent.parent / 'files'

if __name__ == '__main__':
    RoomDB.create_table()
    StudentDB.create_table()
    fh = JsonHandler(FILE_PATH / 'rooms.json')
    rooms = RoomDB.from_list(fh.read_file())
    for room in rooms:
        room.insert()
    fh = JsonHandler(FILE_PATH / 'students.json')
    students = StudentDB.from_list(fh.read_file())
    for student in students:
        student.insert()
    RoomDB.get_student_num_list()
    RoomDB.get_top_5_rooms_with_smallest_average_student_age()
    RoomDB.get_top_5_rooms_with_largest_age_difference_among_students()
    RoomDB.get_list_of_rooms_where_students_of_different_sexes_live_together()
