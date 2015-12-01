#+
# Copyright 2014 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#####################################################################


import copy


class Environment(dict):
    def __init__(self, context, outer=None, iterable=None):
        super(Environment, self).__init__()
        self.context = context
        self.outer = outer
        if iterable:
            for k, v in iterable:
                self[k] = v

    def find(self, var):
        if var in self:
            return self[var]

        if self.outer:
            return self.outer.find(var)

        if var in self.context.controls:
            return ControlFunction(self.context, var, self.context.controls.get(var))

        if var in self.context.builtins:
            return BuiltinFunction(self.context, var, self.context.builtins.get(var))


class Function(object):
    def __init__(self, context, parms, exp, env):
        self.context = context
        self.parms = parms
        self.exp = exp
        self.env = env

    def __call__(self, *args):
        return self.context.eval(copy.deepcopy(self.exp), Environment(
            self.context,
            outer=self.env,
            iterable=zip(self.parms, args)
        ))


class BuiltinFunction(object):
    def __init__(self, context, name, f):
        self.context = context
        self.name = name
        self.f = f

    def __call__(self, *args):
        return self.f(*args)

    def __str__(self):
        return "<builtin function '{0}'>".format(self.name)

    def __repr__(self):
        return str(self)


class ControlFunction(object):
    def __init__(self, context, name, f):
        self.context = context
        self.name = name
        self.f = f

    def __call__(self, exprs, env):
        return self.f(self.context, exprs, env)

    def __str__(self):
        return "<control function '{0}'>".format(self.name)

    def __repr__(self):
        return str(self)
