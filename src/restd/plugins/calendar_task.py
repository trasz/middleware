from base import CRUDBase


class CalendarTaskCRUD(CRUDBase):
    namespace = 'calendar_task'


def _init(rest):
    rest.register_crud(CalendarTaskCRUD)
