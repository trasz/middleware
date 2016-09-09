/*+
 * Copyright 2016 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pwd.h>
#include <grp.h>
#include <nsswitch.h>
#include <jansson.h>
#include <dispatcher.h>
#include <netdb.h>

#define PASSWD_FILE "/etc/passwd.json"
#define GROUP_FILE  "/etc/group.json"

static char *alloc_string(char **, size_t *, const char *);
static void *alloc_blob(char **, size_t *, size_t);
static void *alloc_null(char **, size_t *);
static void flat_load_files();
static json_t *flat_find_user(const char *, const char *, uid_t);
static json_t *flat_find_group(const char *, const char *, gid_t);
static int call_dispatcher(const char *, json_t *, json_t **);
static void populate_user(json_t *, struct passwd *, char *, size_t);
static void populate_group(json_t *, struct group *, char *, size_t);
static void populate_hostent(json_t *, struct hostent *, char *, size_t, int);
static int gr_addgid(gid_t, gid_t *, int, int *);
static void aiforaf(const char *, int, struct addrinfo *, struct addrinfo **);

NSS_METHOD_PROTOTYPE(nss_freenas_getpwnam_r);
NSS_METHOD_PROTOTYPE(nss_freenas_getpwuid_r);
NSS_METHOD_PROTOTYPE(nss_freenas_getpwent_r);
NSS_METHOD_PROTOTYPE(nss_freenas_setpwent);
NSS_METHOD_PROTOTYPE(nss_freenas_endpwent);
NSS_METHOD_PROTOTYPE(nss_freenas_getgrnam_r);
NSS_METHOD_PROTOTYPE(nss_freenas_getgrgid_r);
NSS_METHOD_PROTOTYPE(nss_freenas_getgrent_r);
NSS_METHOD_PROTOTYPE(nss_freenas_setgrent);
NSS_METHOD_PROTOTYPE(nss_freenas_endgrent);
NSS_METHOD_PROTOTYPE(nss_freenas_getgroupmembership);
NSS_METHOD_PROTOTYPE(nss_freenas_getaddrinfo);
NSS_METHOD_PROTOTYPE(nss_freenas_gethostbyaddr_r);
NSS_METHOD_PROTOTYPE(nss_freenas_gethostbyname2_r);
NSS_METHOD_PROTOTYPE(nss_freenas_ghbyaddr);
NSS_METHOD_PROTOTYPE(nss_freenas_ghbyname);

static json_t *flat_users;
static json_t *flat_groups;
static connection_t *conn = NULL;

static int pw_idx = 0;
static json_t *pw_results = NULL;
static rpc_call_t *pw_call = NULL;

static int gr_idx = 0;
static json_t *gr_results = NULL;
static rpc_call_t *gr_call = NULL;

static char *
alloc_string(char **buf, size_t *max, const char *str)
{
	size_t length;
	char *ret = *buf;

	if (str == NULL)
		str = "";

	length = strlen(str) + 1;

	if (length > *max)
		return (NULL);

	if (length > 0)
		memcpy(*buf, str, length);

	*buf += length;
	*max -= length;

	return (ret);
}

static void *
alloc_blob(char **buf, size_t *max, size_t length)
{
	char *ret;

	if (length > *max)
		return (NULL);

	ret = *buf;
	*buf += length;
	*max -= length;
	return (ret);
}

static void *
alloc_null(char **buf, size_t *max)
{
	char *ret;

	ret = *buf;
	if (*max < sizeof(void *))
		return (NULL);

	memset(*buf, 0, sizeof(void *));
	*buf += sizeof(void *);
	*max -= sizeof(void *);
	return (ret);
}

static void
flat_load_files()
{
	json_error_t err;

	flat_users = json_load_file(PASSWD_FILE, 0, &err);
	flat_groups = json_load_file(GROUP_FILE, 0, &err);
}

static json_t *
flat_find_user(const char *name, const char *id, uid_t uid)
{
	json_t *user;
	json_t *val;
	size_t index;

	if (flat_users == NULL)
	flat_load_files();

	/* Bail out if still null */
	if (flat_users == NULL)
		return (NULL);

	json_array_foreach(flat_users, index, user) {
	if (name != NULL || id != NULL) {
		/* Search by name or id */
		val = json_object_get(user, name != NULL ? "username" : "id");
		if (val == NULL)
			continue;

		if (strcmp(json_string_value(val),
		    name != NULL ? name : id) == 0) {
			json_incref(user);
			return (user);
		}
	} else {
		/* Search by uid */
		val = json_object_get(user, "uid");
		if (val == NULL)
			continue;

		if (json_integer_value(val) == uid) {
			json_incref(user);
			return (user);
		}
	}
	}

	return (NULL);
}

static json_t *
flat_find_group(const char *name, const char *id, gid_t gid)
{
	json_t *group;
	json_t *val;
	size_t index;

	if (flat_groups == NULL)
	flat_load_files();

	/* Bail out if still null */
	if (flat_users == NULL)
		return (NULL);

	json_array_foreach(flat_groups, index, group) {
	if (name != NULL || id != NULL) {
		/* Search by name or id */
		val = json_object_get(group, name != NULL ? "name" : "id");
		if (val == NULL)
			continue;

		if (strcmp(json_string_value(val),
		    name != NULL ? name : id)== 0) {
			json_incref(group);
			return (group);
		}
	} else {
		/* Search by gid */
		val = json_object_get(group, "gid");
		if (val == NULL)
			continue;

		if (json_integer_value(val) == gid) {
			json_incref(group);
			return (group);
		}
	}
	}

	return (NULL);
}

static void
populate_user(json_t *user, struct passwd *pwbuf, char *buf, size_t buflen)
{
	json_t *obj;

	obj = json_object_get(user, "username");
	pwbuf->pw_name = alloc_string(&buf, &buflen, json_string_value(obj));

	obj = json_object_get(user, "uid");
	pwbuf->pw_uid = json_integer_value(obj);

	obj = json_object_get(user, "gid");
	pwbuf->pw_gid = json_integer_value(obj);

	obj = json_object_get(user, "full_name");
	pwbuf->pw_gecos = alloc_string(&buf, &buflen,
	    json_string_value(obj));

	obj = json_object_get(user, "shell");
	pwbuf->pw_shell = alloc_string(&buf, &buflen,
	    json_string_value(obj));

	obj = json_object_get(user, "home");
	pwbuf->pw_dir = alloc_string(&buf, &buflen,
	    json_string_value(obj));

	obj = json_object_get(user, "unixhash");
	pwbuf->pw_passwd = alloc_string(&buf, &buflen,
	    obj != NULL ? json_string_value(obj) : "*");

	pwbuf->pw_class = alloc_string(&buf, &buflen, "default");
}

static void
populate_group(json_t *group, struct group *grbuf, char *buf, size_t buflen)
{
	json_t *obj;

	obj = json_object_get(group, "name");
	grbuf->gr_name = alloc_string(&buf, &buflen, json_string_value(obj));

	obj = json_object_get(group, "gid");
	grbuf->gr_gid = json_integer_value(obj);

	obj = json_object_get(group, "unixhash");
	grbuf->gr_passwd = alloc_string(&buf, &buflen,
	obj != NULL ? json_string_value(obj) : "*");

	grbuf->gr_mem = alloc_null(&buf, &buflen);
}

static void
populate_hostent(json_t *result, struct hostent *hp, char *buf, size_t buflen,
    int af)
{
	json_t *addrs, *aliases, *i;
	size_t idx, addrs_cnt, aliases_cnt, addrlen;
	struct in6_addr addr;

	addrs = json_object_get(result, "addresses");
	if (addrs == NULL)
		return;

	aliases = json_object_get(result, "aliases");
	if (aliases == NULL)
		return;

	hp->h_addr_list = alloc_blob(&buf, &buflen,
	    (json_array_size(addrs) + 1) * sizeof(void *));
	hp->h_aliases = alloc_blob(&buf, &buflen,
	    (json_array_size(aliases) + 1) * sizeof(void *));
	hp->h_name = alloc_string(&buf, &buflen, json_string_value(
	    json_object_get(result, "name")));
	hp->h_length = sizeof(in_addr_t);
	hp->h_addrtype = af;

	json_array_foreach(addrs, idx, i) {
		if (af == AF_INET)
			addrlen = sizeof(struct in_addr);

		if (af == AF_INET6)
			addrlen = sizeof(struct in6_addr);

		inet_pton(af, json_string_value(i), &addr);
		hp->h_addr_list[idx] = alloc_blob(&buf, &buflen, addrlen);
		memcpy(hp->h_addr_list[idx], &addr, addrlen);
	}

	hp->h_addr_list[idx] = NULL;

	json_array_foreach(aliases, idx, i) {
		hp->h_aliases[idx] = alloc_string(&buf, &buflen,
		    json_string_value(i));
	}

	hp->h_aliases[idx] = NULL;
}

static int
call_dispatcher(const char *method, json_t *args, json_t **result)
{
	struct timespec ts;
	struct timeval tv;
	rpc_call_t *call;
	connection_t *conn;

	conn = dispatcher_open("unix:///var/run/dscached.sock");
	if (conn == NULL)
		return (-1);

	gettimeofday(&tv, NULL);
	ts.tv_sec = tv.tv_sec + 2;
	ts.tv_nsec = tv.tv_usec * 1000;

	call = dispatcher_call_async(conn, method, args, NULL, NULL);

	if (call == NULL) {
		dispatcher_close(conn);
		return (-1);
	}

	if (rpc_call_timedwait(call, &ts) != 0) {
		dispatcher_close(conn);
		return (-1);
	}

	if (rpc_call_success(call) != RPC_CALL_DONE) {
		dispatcher_close(conn);
		return (-1);
	}

	*result = rpc_call_result(call);
	json_incref(*result);
	dispatcher_close(conn);
	return (0);
}

static rpc_call_t *
call_dispatcher_stream(const char *method, json_t *args, json_t **result)
{
        struct timespec ts;
        rpc_call_t *call;

        conn = dispatcher_open("unix:///var/run/dscached.sock");
        if (conn == NULL)
                return (NULL);

        call = dispatcher_call_sync_ex(conn, method, args);
        if (call == NULL) {
                dispatcher_close(conn);
                return (NULL);
        }

        *result = rpc_call_result(call);
        return (call);
}

static int
gr_addgid(gid_t gid, gid_t *groups, int maxgrp, int *grpcnt)
{
	int ret, dupc;

	/* skip duplicates */
	for (dupc = 0; dupc < MIN(maxgrp, *grpcnt); dupc++) {
		if (groups[dupc] == gid)
			return 1;
	}

	ret = 1;
	if (*grpcnt < maxgrp)
		groups[*grpcnt] = gid;
	else
		ret = 0;

	(*grpcnt)++;

	return ret;
}

int
nss_freenas_getpwnam_r(void *retval, void *mdata, va_list ap)
{
	struct passwd *pwbuf;
	const char *name;
	char *buf;
	size_t buflen;
	json_t *user;
	int *ret;

	name = va_arg(ap, const char *);
	pwbuf = va_arg(ap, struct passwd *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

	if (call_dispatcher("dscached.account.getpwnam", json_pack("[s]", name),
	    &user) < 0)
		user = flat_find_user(name, NULL, -1);

	if (user == NULL || json_is_null(user)) {
		if (user != NULL)
			json_decref(user);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_user(user, pwbuf, buf, buflen);

	*(struct passwd **)retval = pwbuf;
	*ret = 0;

	json_decref(user);
	return (NS_SUCCESS);
}

int
nss_freenas_getpwuid_r(void *retval, void *mdata, va_list ap)
{
	struct passwd *pwbuf;
	uid_t uid;
	char *buf;
	size_t buflen;
	json_t *user;
	int *ret;

	uid = va_arg(ap, uid_t);
	pwbuf = va_arg(ap, struct passwd *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

	if (call_dispatcher("dscached.account.getpwuid", json_pack("[i]", uid),
	    &user) < 0)
		user = flat_find_user(NULL, NULL, uid);

	if (user == NULL || json_is_null(user)) {
		if (user != NULL)
			json_decref(user);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_user(user, pwbuf, buf, buflen);

	*(struct passwd **)retval = pwbuf;
	*ret = 0;

	json_decref(user);
	return (NS_SUCCESS);
}

int
nss_freenas_getpwent_r(void *retval, void *mdata, va_list ap)
{
	struct passwd *pwbuf;
	char *buf;
	const char *shell;
	int *ret;
	size_t buflen;
	json_t *user;

	pwbuf = va_arg(ap, struct passwd *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

        if (pw_idx >= json_array_size(pw_results) && pw_call) {
                if (rpc_call_continue(pw_call, true) != RPC_CALL_MORE_AVAILABLE) {
                        rpc_call_free(pw_call);
                        pw_call = NULL;
                        pw_results = NULL;
                        return (NS_NOTFOUND);
                }

                pw_results = rpc_call_result(pw_call);
                pw_idx = 0;
        }

	user = json_array_get(pw_results, pw_idx);

	if (user == NULL) {
		if (pw_results != NULL)
			json_decref(pw_results);

		pw_idx = 0;
		pw_results = NULL;
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_user(user, pwbuf, buf, buflen);
	*(struct passwd **)retval = pwbuf;
	*ret = 0;

	pw_idx++;
	return (NS_SUCCESS);
}

int
nss_freenas_setpwent(void *retval, void *mdata, va_list ap)
{
	pw_call = call_dispatcher_stream("dscached.account.query", json_array(),
            &pw_results);

        if (pw_call == NULL) {
		flat_load_files();
		pw_results = json_copy(flat_users);
	}

	if (json_is_null(pw_results)) {
		json_decref(pw_results);
		pw_results = NULL;
		return (NS_NOTFOUND);
	}

	pw_idx = 0;
	return (NS_SUCCESS);
}

int
nss_freenas_endpwent(void *retval, void *mdata, va_list ap)
{
	if (pw_results != NULL)
	json_decref(pw_results);

	pw_idx = 0;
	pw_results = NULL;

	if (conn != NULL) {
		dispatcher_close(conn);
		conn = NULL;
	}

	return (NS_SUCCESS);
}

int
nss_freenas_getgrnam_r(void *retval, void *mdata __unused, va_list ap)
{
	struct group *grbuf;
	const char *name;
	char *buf;
	size_t buflen;
	json_t *group;
	int *ret;

	name = va_arg(ap, const char *);
	grbuf = va_arg(ap, struct group *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

	if (call_dispatcher("dscached.group.getgrnam", json_pack("[s]", name),
	    &group) < 0)
		group = flat_find_group(name, NULL, -1);

	if (group == NULL || json_is_null(group)) {
		if (group != NULL)
			json_decref(group);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_group(group, grbuf, buf, buflen);

	*(struct group **)retval = grbuf;
	*ret = 0;

	json_decref(group);
	return (NS_SUCCESS);
}

int
nss_freenas_getgrgid_r(void *retval, void *mdata __unused, va_list ap)
{
	struct group *grbuf;
	gid_t gid;
	char *buf;
	size_t buflen;
	json_t *group;
	int *ret;

	gid = va_arg(ap, gid_t);
	grbuf = va_arg(ap, struct group *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

	if (call_dispatcher("dscached.group.getgrgid", json_pack("[i]", gid),
	    &group) < 0)
		group = flat_find_group(NULL, NULL, gid);

	if (group == NULL || json_is_null(group)) {
		if (group != NULL)
			json_decref(group);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_group(group, grbuf, buf, buflen);

	*(struct group **)retval = grbuf;
	*ret = 0;

	json_decref(group);
	return (NS_SUCCESS);
}

int
nss_freenas_getgrent_r(void *retval, void *mdata, va_list ap)
{
	struct group *grbuf;
	char *buf;
	const char *shell;
	int *ret;
	size_t buflen;
	json_t *group;

	grbuf = va_arg(ap, struct group *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret = va_arg(ap, int *);

        if (gr_idx >= json_array_size(gr_results) && gr_call) {
                if (rpc_call_continue(gr_call, true) != RPC_CALL_MORE_AVAILABLE) {
                        rpc_call_free(gr_call);
                        gr_call = NULL;
                        gr_results = NULL;
                        return (NS_NOTFOUND);
                }

                gr_results = rpc_call_result(gr_call);
                gr_idx = 0;
        }

	group = json_array_get(gr_results, gr_idx);

	if (group == NULL) {
		if (gr_results != NULL)
			json_decref(gr_results);

		gr_idx = 0;
		gr_results = NULL;
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_group(group, grbuf, buf, buflen);
	*(struct group **)retval = grbuf;
	*ret = 0;

	gr_idx++;

	return (NS_SUCCESS);
}

int
nss_freenas_setgrent(void *retval, void *mdata, va_list ap)
{
        gr_call = call_dispatcher_stream("dscached.group.query", json_array(),
                      &gr_results);

        if (gr_call == NULL) {
		flat_load_files();
		gr_results = json_copy(flat_groups);
	}

	if (json_is_null(gr_results)) {
		json_decref(gr_results);
		gr_results = NULL;
		return (NS_NOTFOUND);
	}

	pw_idx = 0;
	json_incref(gr_results);
	return (NS_SUCCESS);
}

int
nss_freenas_endgrent(void *retval, void *mdata, va_list ap)
{
	if (gr_results != NULL)
		json_decref(pw_results);

	pw_idx = 0;
	pw_results = NULL;

	if (conn != NULL) {
		dispatcher_close(conn);
		conn = NULL;
	}

	return (NS_SUCCESS);
}

int
nss_freenas_getgroupmembership(void *retval, void *mdata, va_list ap)
{
	const char *uname;
	gid_t group;
	gid_t *groups;
	int maxgrp;
	int *groupc;
	int *ret;
	json_t *membership, *gid;
	size_t idx;

	uname = va_arg(ap, const char *);
	group = va_arg(ap, gid_t);
	groups = va_arg(ap, gid_t *);
	maxgrp = va_arg(ap, int);
	groupc = va_arg(ap, int *);

	if (call_dispatcher("dscached.account.getgroupmembership",
            json_pack("[s]", uname), &membership) < 0) {
                gr_addgid(group, groups, maxgrp, groupc);
                return (NS_SUCCESS);
        }

	if (membership == NULL || json_is_null(membership)) {
		if (membership != NULL)
			json_decref(membership);
		*groupc = 0;
		return (NS_NOTFOUND);
	}

        gr_addgid(group, groups, maxgrp, groupc);

	json_array_foreach(membership, idx, gid) {
		gr_addgid(json_integer_value(gid), groups, maxgrp, groupc);
	}

	return (NS_SUCCESS);
}

/*
 * Stolen from nss_mdns_freebsd.c in FreeBSD Ports collection
 *
 * Original file name: dns/mDNSResponder_nss/files/nss_mdns_freebsd.c
 * Original author: Kurt Jaeger
 */
static void
aiforaf(const char *name, int af, struct addrinfo *pai, struct addrinfo **aip)
{
        struct hostent host;
        char hostbuf[8*1024];
        char **addrp;
        char addrstr[INET6_ADDRSTRLEN];
        struct addrinfo hints, *res0, *res;
        json_t *result;

        if (call_dispatcher("dscached.host.gethostbyname", json_pack("[si]",
            name, af), &result) < 0) {
                return;
        }

        if (result == NULL || json_is_null(result)) {
                if (result != NULL)
                        json_decref(result);
                return;
        }

        populate_hostent(result, &host, hostbuf, sizeof(hostbuf), af);

        for (addrp = host.h_addr_list; *addrp; addrp++) {
                /* XXX this sucks, but get_ai is not public */
                if (!inet_ntop(host.h_addrtype, *addrp, addrstr,
                    sizeof(addrstr)))
                        continue;
                hints = *pai;
                hints.ai_flags = AI_NUMERICHOST;
                hints.ai_family = af;
                if (getaddrinfo(addrstr, NULL, &hints, &res0))
                        continue;

                for (res = res0; res; res = res->ai_next)
                        res->ai_flags = pai->ai_flags;

                (*aip)->ai_next = res0;
                while ((*aip)->ai_next)
                        *aip = (*aip)->ai_next;
        }
}

int
nss_freenas_getaddrinfo(void *retval, void *mdata, va_list ap)
{
        struct addrinfo sentinel, *cur;
        const char *name;
        struct addrinfo *ai;

        name  = va_arg(ap, char *);
        ai = va_arg(ap, struct addrinfo *);

        memset(&sentinel, 0, sizeof(sentinel));
        cur = &sentinel;

        if ((ai->ai_family == AF_UNSPEC) || (ai->ai_family == AF_INET6))
                aiforaf(name, AF_INET6, ai, &cur);

        if ((ai->ai_family == AF_UNSPEC) || (ai->ai_family == AF_INET))
                aiforaf(name, AF_INET, ai, &cur);

        if (!sentinel.ai_next) {
                h_errno = HOST_NOT_FOUND;
                return (NS_NOTFOUND);
        }

        *((struct addrinfo **)retval) = sentinel.ai_next;
        return (NS_SUCCESS);
}

int
nss_freenas_gethostbyaddr_r(void *retval, void *mdata, va_list ap)
{
	void		*addr;
	char		*buf;
	struct hostent	*hp;
	struct hostent	**resultp;
	int		 af;
	size_t		 buflen;
	int		 len;
	int		 ret_errno;
	int *ret;
	char addrstr[INET6_ADDRSTRLEN];
	json_t *result;

	addr = va_arg(ap, void *);
	len = va_arg(ap, socklen_t);
	af = va_arg(ap, int);
	hp = va_arg(ap, struct hostent *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret_errno = va_arg(ap, int);
	ret = va_arg(ap, int *);
	resultp = (struct hostent **)retval;

	if (inet_ntop(af, addr, addrstr, INET6_ADDRSTRLEN) == NULL)
		return (NS_UNAVAIL);

	if (call_dispatcher("dscached.host.gethostbyaddr", json_pack("[si]",
	    addrstr, af), &result) < 0) {
		return (NS_UNAVAIL);
	}

	if (result == NULL || json_is_null(result)) {
		if (result != NULL)
			json_decref(result);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_hostent(result, hp, buf, buflen, af);

	*resultp = hp;
	*ret = 0;
	json_decref(result);
	return (NS_SUCCESS);
}

int
nss_freenas_gethostbyname2_r(void *retval, void *mdata, va_list ap)
{
	char *buf;
	const char *name;
	struct hostent *hp;
	struct hostent **resultp;
	int af;
	size_t buflen;
	int ret_errno;
	int *ret;
	json_t *result, *addrs, *aliases, *i;
	size_t idx;
	in_addr_t addr;

	name = va_arg(ap, char *);
	af = va_arg(ap, int);
	hp = va_arg(ap, struct hostent *);
	buf = va_arg(ap, char *);
	buflen = va_arg(ap, size_t);
	ret_errno = va_arg(ap, int);
	ret = va_arg(ap, int *);
	resultp = (struct hostent **)retval;

	if (call_dispatcher("dscached.host.gethostbyname", json_pack("[si]",
	    name, af), &result) < 0) {
		return (NS_UNAVAIL);
	}

	if (result == NULL || json_is_null(result)) {
		if (result != NULL)
			json_decref(result);
		*ret = ENOENT;
		return (NS_NOTFOUND);
	}

	populate_hostent(result, hp, buf, buflen, af);

	*resultp = hp;
	*ret = 0;
	json_decref(result);
	return (NS_SUCCESS);
}

int
nss_freenas_ghbyaddr(void *retval, void *mdata, va_list ap)
{
	return (NS_UNAVAIL);
}

int
nss_freenas_ghbyname(void *retval, void *mdata, va_list ap)
{
	return (NS_UNAVAIL);
}

static ns_mtab methods[] = {
	{ NSDB_PASSWD, "getpwnam_r", nss_freenas_getpwnam_r, NULL },
	{ NSDB_PASSWD, "getpwuid_r", nss_freenas_getpwuid_r, NULL },
	{ NSDB_PASSWD, "getpwent_r", nss_freenas_getpwent_r, NULL },
	{ NSDB_PASSWD, "endpwent",   nss_freenas_endpwent,   NULL },
	{ NSDB_PASSWD, "setpwent",   nss_freenas_setpwent,   NULL },
	{ NSDB_GROUP,  "getgrnam_r", nss_freenas_getgrnam_r, NULL },
	{ NSDB_GROUP,  "getgrgid_r", nss_freenas_getgrgid_r, NULL },
	{ NSDB_GROUP,  "getgrent_r", nss_freenas_getgrent_r, NULL },
	{ NSDB_GROUP,  "endgrent",   nss_freenas_endgrent,   NULL },
	{ NSDB_GROUP,  "setgrent",   nss_freenas_setgrent,   NULL },
	{ NSDB_GROUP,  "getgroupmembership", nss_freenas_getgroupmembership, NULL },
	{ NSDB_HOSTS,  "getaddrinfo", nss_freenas_getaddrinfo, NULL },
	{ NSDB_HOSTS,  "gethostbyaddr_r", nss_freenas_gethostbyaddr_r, NULL },
	{ NSDB_HOSTS,  "gethostbyname2_r", nss_freenas_gethostbyname2_r, NULL },
	{ NSDB_HOSTS,  "ghbyaddr", nss_freenas_ghbyaddr, NULL },
	{ NSDB_HOSTS,  "ghbyname", nss_freenas_ghbyname, NULL },
};

ns_mtab *
nss_module_register(const char *name, unsigned int *size,
	nss_module_unregister_fn *unregister)
{
	*size = sizeof(methods) / sizeof(methods[0]);
	*unregister = NULL;
	return (methods);
}
