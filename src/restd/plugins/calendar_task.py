from base import CRUDBase, Resource


class CommandResource(Resource):
    name = 'command'
    post = 'task:calendar_task.command'

    def run_post(self, req, urlparams):
        return req.context['doc']


class RunResource(Resource):
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
