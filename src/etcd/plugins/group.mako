<%
    def get_name(id):
        user = dispatcher.call_sync('user.query', [('id', '=', id)], {'single': True})
        return user['username']

    def members(group):
        return ','.join([get_name(i) for i in group['members']])
%>\
% for group in dispatcher.call_sync("group.query"):
${group['name']}:*:${group['id']}:${members(group)}
% endfor
