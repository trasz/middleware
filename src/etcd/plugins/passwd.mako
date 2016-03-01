% for user in ds.query("users"):
${user['username']}:*:${user['uid']}:${user['group']}:${user['full_name']}:${user['home']}:${user['shell']}
% endfor