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
#include <sys/param.h>
#include <pwd.h>
#include <grp.h>
#include <nsswitch.h>
#include <jansson.h>
#include <dispatcher.h>

static char *alloc_string(char **, size_t *, const char *);
static void *alloc_null(char **, size_t *);
static int call_dispatcher(const char *, json_t *, json_t **, bool ref);
static void populate_user(json_t *, struct passwd *, char *, size_t);
static void populate_group(json_t *, struct group *, char *, size_t);
static int gr_addgid(gid_t, gid_t *, int, int *);

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

static int pw_idx = 0;
static json_t *pw_results = NULL;

static int gr_idx = 0;
static json_t *gr_results = NULL;

static char *
alloc_string(char **buf, size_t *max, const char *str)
{
    size_t length;
    char *ret;

    ret = *buf;
    length = strlen(str) + 1;

    if (length > *max)
        return (NULL);

    memcpy(*buf, str, length);
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
    if (obj != NULL)
        pwbuf->pw_gecos = alloc_string(&buf, &buflen, json_string_value(obj));

    obj = json_object_get(user, "shell");
    if (obj != NULL)
        pwbuf->pw_shell = alloc_string(&buf, &buflen, json_string_value(obj));

    obj = json_object_get(user, "home");
    if (obj != NULL)
        pwbuf->pw_dir = alloc_string(&buf, &buflen, json_string_value(obj));

    obj = json_object_get(user, "unixhash");
    if (obj != NULL)
        pwbuf->pw_passwd = alloc_string(&buf, &buflen, json_string_value(obj));
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

static int
call_dispatcher(const char *method, json_t *args, json_t **result, bool ref)
{
    connection_t *conn;

    conn = dispatcher_open("unix");
    if (conn == NULL)
        return (-1);

    if (dispatcher_login_service(conn, "nss-freenas") < 0) {
        dispatcher_close(conn);
        return (-1);
    }

    if (dispatcher_call_sync(conn, method, args, result) < 0) {
        dispatcher_close(conn);
        return (NS_UNAVAIL);
    }

    if (ref)
        json_incref(*result);

    dispatcher_close(conn);
    return (0);
}

static int
gr_addgid(gid_t gid, gid_t *groups, int maxgrp, int *grpcnt)
{
	int	ret, dupc;

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
        &user, false) < 0)
        return (NS_UNAVAIL);

    if (json_is_null(user)) {
        *ret = ENOENT;
        return (NS_NOTFOUND);
    }

    populate_user(user, pwbuf, buf, buflen);

    *(struct passwd **)retval = pwbuf;
    *ret = 0;

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
        &user, false) < 0)
        return (NS_UNAVAIL);

    if (json_is_null(user)) {
        *ret = ENOENT;
        return (NS_NOTFOUND);
    }

    populate_user(user, pwbuf, buf, buflen);

    *(struct passwd **)retval = pwbuf;
    *ret = 0;

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
    if (call_dispatcher("dscached.account.query", json_array(),
        &pw_results, true) < 0)
        return (NS_UNAVAIL);

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
    grbuf = va_arg(ap, struct passwd *);
    buf = va_arg(ap, char *);
    buflen = va_arg(ap, size_t);
    ret = va_arg(ap, int *);

    if (call_dispatcher("dscached.group.getgrnam", json_pack("[s]", name),
        &group, false) < 0)
        return (NS_UNAVAIL);

    if (json_is_null(group)) {
        *ret = ENOENT;
        return (NS_NOTFOUND);
    }

    populate_group(group, grbuf, buf, buflen);

    *(struct group **)retval = grbuf;
    *ret = 0;

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
        &group, false) < 0)
        return (NS_UNAVAIL);

    if (json_is_null(group)) {
        *ret = ENOENT;
        return (NS_NOTFOUND);
    }

    populate_group(group, grbuf, buf, buflen);

    *(struct group **)retval = grbuf;
    *ret = 0;

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
    if (call_dispatcher("dscached.group.query", json_array(),
        &gr_results, true) < 0)
        return (NS_UNAVAIL);

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

    return (NS_SUCCESS);
}

int
nss_freenas_getgroupmembership(void *retval, void *mdata, va_list ap)
{
/*
	const char 	*uname  = va_arg(ap, const char *);
	gid_t		 group  = va_arg(ap, gid_t);
	gid_t		*groups = va_arg(ap, gid_t *);
	int		 maxgrp = va_arg(ap, int);
	int		*groupc = va_arg(ap, int *);
*/
    return (NS_UNAVAIL);
}

int
nss_freenas_getaddrinfo(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_gethostbyaddr_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_gethostbyname2_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
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
