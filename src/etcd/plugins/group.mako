<%
    def get_name(uid):
        user = dispatcher.call_sync('user.query', [('uid', '=', uid)], {'single': True})
        return user['username']

    def members(group):
        return ','.join([get_name(i) for i in group['members']])
%>\
% for group in dispatcher.call_sync("group.query"):
${group['name']}:*:${group['gid']}:${members(group)}
% endfor
