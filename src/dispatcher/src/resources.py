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


import logging
import networkx as nx
from gevent.lock import RLock
from freenas.utils.trace_logger import TRACE


class Resource(object):
    def __init__(self, name):
        self.name = name
        self.busy = False

    def __str__(self):
        return "<Resource '{0}'>".format(self.name)

    def __repr__(self):
        return str(self)


class ResourceError(RuntimeError):
    pass


class ResourceGraph(object):
    def __init__(self):
        self.logger = logging.getLogger('ResourceGraph')
        self.mutex = RLock()
        self.root = Resource('root')
        self.resources = nx.DiGraph()
        self.resources.add_node(self.root)

    def lock(self):
        self.mutex.acquire()

    def unlock(self):
        self.mutex.release()

    @property
    def nodes(self):
        return self.resources.nodes()

    def add_resource(self, resource, parents=None):
        with self.mutex:
            if not resource:
                raise ResourceError('Invalid resource')
    
            if self.get_resource(resource.name):
                raise ResourceError('Resource {0} already exists'.format(resource.name))
    
            self.resources.add_node(resource)
            if not parents:
                parents = ['root']
    
            for p in parents:
                node = self.get_resource(p)
                if not node:
                    raise ResourceError('Invalid parent resource {0}'.format(p))
    
                self.resources.add_edge(node, resource)

    def remove_resource(self, name):
        with self.mutex:
            resource = self.get_resource(name)
    
            if not resource:
                return
    
            for i in nx.descendants(self.resources, resource):
                self.resources.remove_node(i)
    
            self.resources.remove_node(resource)

    def remove_resources(self, names):
        with self.mutex:
            for name in names:
                resource = self.get_resource(name)
    
                if not resource:
                    return
    
                for i in nx.descendants(self.resources, resource):
                    self.resources.remove_node(i)
    
                self.resources.remove_node(resource)

    def update_resource(self, name, new_parents):
        with self.mutex:
            resource = self.get_resource(name)
    
            if not resource:
                return
    
            for i in self.resources.predecessors(resource):
                self.resources.remove_edge(i, resource)
    
            for p in new_parents:
                node = self.get_resource(p)
                if not node:
                    raise ResourceError('Invalid parent resource {0}'.format(p))
    
                self.resources.add_edge(node, resource)

    def get_resource(self, name):
        f = [i for i in self.resources.nodes() if i.name == name]
        return f[0] if len(f) > 0 else None

    def get_resource_dependencies(self, name):
        res = self.get_resource(name)
        for i, _ in self.resources.in_edges([res]):
            yield i.name

    def acquire(self, *names):
        if not names:
            return

        with self.mutex:
            self.logger.debug('Acquiring following resources: %s', ','.join(names))
    
            for name in names:
                res = self.get_resource(name)
                if not res:
                    raise ResourceError('Resource {0} not found'.format(name))
    
                for i in nx.descendants(self.resources, res):
                    if i.busy:
                        raise ResourceError('Cannot acquire, some of dependent resources are busy')
    
                res.busy = True

    def can_acquire(self, *names):
        if not names:
            return True

        with self.mutex:
            self.logger.log(TRACE, 'Trying to acquire following resources: %s', ','.join(names))
    
            for name in names:
                res = self.get_resource(name)
                if not res:
                    return False
    
                if res.busy:
                    return False
    
                for i in nx.descendants(self.resources, res):
                    if i.busy:
                        return False
    
            return True

    def release(self, *names):
        if not names:
            return

        with self.mutex:
            self.logger.debug('Releasing following resources: %s', ','.join(names))
    
            for name in names:
                res = self.get_resource(name)
                res.busy = False
