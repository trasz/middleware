#+
# Copyright 2015 iXsystems, Inc.
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

import sys
import operator as op
from .scheme import hashtable, Function
from .output import Table


def read():
    pass


def is_symbol(x):
    from .parser import Symbol
    return isinstance(x, Symbol)


def is_pair(x):
    return x != [] and isinstance(x, list)


def readchar():
    pass


def cons(x, y):
    return [x] + y


def to_string(x):
    return str(x)


def expand(x):
    pass


def load(fn):
    pass


def callcc():
    pass


eof_object = True


functions = {
    '+': op.add,
    '-': op.sub,
    '*': op.mul,
    '/': op.floordiv,
    'not': op.not_,
    'gt': op.gt,
    'lt': op.lt,
    'ge': op.ge,
    'le': op.le,
    'eq': op.eq,
    'equal?': op.eq,
    'eq?': op.is_,
    'length': len,
    'cons': cons,
    'car': lambda x: x[0],
    'cdr': lambda x: x[1:],
    'append': op.add,
    'list': lambda *x: list(x),
    'list?': lambda x: isinstance(x, list),
    'null?': lambda x: not bool(x),
    'boolean?': lambda x: isinstance(x, bool),
    'pair?': is_pair,
    'port?': lambdca x: isinstance(x, None),
    'table?': lambda x: isinstance(x, Table),
    'apply': lambda proc, l:  proc(*l),
    'eval': lambda x: eval(expand(x)),
    'load': lambda fn: load(fn),
    'call/cc': callcc,
    'open-input-file': open,
    'close-input-port': lambda p: p.file.close(),
    'open-output-file': lambda f: open(f, 'w'),
    'close-output-port': lambda p:  p.close(),
    'eof-object?': lambda x: x is eof_object,
    'read-char': readchar,
    'read': read,
    'write': lambda x,port=sys.stdout: port.write(to_string(x)),
    'display': lambda x,port=sys.stdout: port.write(x if isinstance(x, str) else to_string(x))
}


def get_builtins():
    ret = functions
    for func in hashtable.__dict__.values():
        if callable(func) and hasattr(func, 'name'):
            ret[func.name] = func

    return ret


def do_if(context, exprs, env):
    _, test, conseq, alt = exprs
    return conseq if context.eval(test, env) else alt


def do_quote(context, exprs, env):
    from .parser import Literal
    _, exp = exprs
    return Literal(exp, type(exp))


def do_define(context, exprs, env):
    _, var, exp = exprs
    print('defining {0} to {1}'.format(var.name, exp))
    env[var.name] = context.eval(exp, env)


def do_set(context, exprs, env):
    _, var, exp = exprs
    env[var.name] = context.eval(exp, env)


def do_lambda(context, exprs, env):
    _, vars, exp = exprs
    return Function(context, [i.name for i in vars.exprs], exp, env)


def do_begin(context, exprs, env):
    for x in exprs[1:-1]:
        context.eval(x, env)

    return context.eval(exprs[-1], env)


controls = {
    'if': do_if,
    'quote': do_quote,
    'define': do_define,
    'set!': do_set,
    'lambda': do_lambda,
    'begin': do_begin,
}
