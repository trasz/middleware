<%
    def get_gid(user):
        group = ds.get_by_id('groups', user['group'])
        return group['gid'] if group else 65534

%>\
% for user in ds.query("users"):
${user['username']}:*:${user['uid']}:${get_gid(user)}:${user['full_name']}:${user['home']}:${user['shell']}
% endfor