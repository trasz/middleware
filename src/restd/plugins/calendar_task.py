from base import CRUDBase, Resource


class RunResource(Resource):
    name = 'run'
    post = 'task:calendar_task.run'


class CalendarTaskCRUD(CRUDBase):
    namespace = 'calendar_task'
    item_resources = (
        RunResource,
    )


def _init(rest):
    rest.register_crud(CalendarTaskCRUD)
