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
#include <nsswitch.h>
#include <jansson.h>
#include <dispatcher.h>

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

int
nss_freenas_getpwnam_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_getpwuid_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_getpwent_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_setpwent(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_endpwent(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_getgrnam_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_getgrgid_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_getgrent_r(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_setgrent(void *retval, void *mdata, va_list ap)
{
    return (NS_UNAVAIL);
}

int
nss_freenas_endgrent(void *retval, void *mdata, va_list ap)
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
};

ns_mtab *
nss_module_register(const char *name, unsigned int *size, nss_module_unregister_fn *unregister)
{
    *size = sizeof(methods) / sizeof(methods[0]);
    *unregister = NULL;
    return (methods);
}