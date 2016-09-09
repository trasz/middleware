from base import CRUDBase, ItemResource, Resource


class CommandResource(Resource):
    name = 'command'
    post = 'task:calendar_task.command'


class RunResource(ItemResource):
    name = 'run'
    post = 'task:calendar_task.run'


class CalendarTaskCRUD(CRUDBase):
    namespace = 'calendar_task'
    entity_resources = (
        CommandResource,
    )
    item_resources = (
        RunResource,
    )


def _init(rest):
    rest.register_crud(CalendarTaskCRUD)
