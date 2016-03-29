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

#include <sys/param.h>
#include <security/pam_appl.h>
#include <security/pam_modules.h>
#include <security/pam_mod_misc.h>
#include <jansson.h>
#include <dispatcher.h>

#define PASSWORD_PROMPT "Password:"

static int
call_dispatcher(const char *method, json_t *args, json_t **result)
{
    connection_t *conn;

    conn = dispatcher_open("unix");
    if (conn == NULL)
        return (-1);

    if (dispatcher_login_service(conn, "pam-freenas") < 0) {
        dispatcher_close(conn);
        return (-1);
    }

    if (dispatcher_call_sync(conn, method, args, result) != RPC_CALL_DONE) {
        dispatcher_close(conn);
        return (-1);
    }

    json_incref(*result);
    dispatcher_close(conn);
    return (0);
}

PAM_EXTERN int
pam_sm_setcred(struct pam_handle *pamh, int flags, int argc, const char *argv[])
{

    return (PAM_SUCCESS);
}

PAM_EXTERN int
pam_sm_acct_mgmt(struct pam_handle *pamh, int flags, int argc, const char *argv[])
{

    return (PAM_SUCCESS);
}

PAM_EXTERN int
pam_sm_authenticate(struct pam_handle *pamh, int flags, int argc, const char *argv[])
{
    const char *user, *password;
    char *result_s;
    json_t *result;
    int err;

    err = pam_get_user(pamh, &user, NULL);
    if (err != PAM_SUCCESS)
        return (err);

    PAM_LOG("Got user: %s", user);

    err = pam_get_authtok(pamh, PAM_AUTHTOK, &password, PASSWORD_PROMPT);
    if (err != PAM_SUCCESS)
        return (err);

    PAM_LOG("Got password");

    if (call_dispatcher("dscached.account.authenticate", json_pack("[ss]", user, password), &result) != 0) {
        PAM_LOG("Cannot call dispatcher");
        return (PAM_SERVICE_ERR);
    }

    result_s = json_dumps(result, 0);
    PAM_LOG("Result: %s", result_s);
    free(result_s);

    if (json_is_true(result))
        return (PAM_SUCCESS);

    return (PAM_PERM_DENIED);
}

PAM_MODULE_ENTRY("pam_freenas");
