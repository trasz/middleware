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

from gevent.event import Event
from gevent.lock import RLock
from freenas.utils.query import wrap


class CacheStore(object):
    class CacheItem(object):
        def __init__(self):
            self.valid = Event()
            self.data = None

    def __init__(self):
        self.lock = RLock()
        self.store = {}

    def __getitem__(self, item):
        return self.get(item)

    def put(self, key, data):
        with self.lock:
            item = self.store[key] if key in self.store else self.CacheItem()
            item.data = data
            item.valid.set()

            if key not in self.store:
                self.store[key] = item
                return True

            return False

    def get(self, key, default=None, timeout=None):
        item = self.store.get(key)
        if item:
            item.valid.wait(timeout)
            return item.data

        return default

    def remove(self, key):
        with self.lock:
            if key in self.store:
                del self.store[key]
                return True

            return False

    def exists(self, key):
        return key in self.store

    def rename(self, oldkey, newkey):
        with self.lock:
            obj = self.get(oldkey)
            obj['id'] = newkey
            self.put(newkey, obj)
            self.remove(oldkey)

    def is_valid(self, key):
        item = self.store.get(key)
        if item:
            return item.valid.is_set()

        return False

    def invalidate(self, key):
        with self.lock:
            item = self.store.get(key)
            if item:
                item.valid.clear()

    def itervalid(self):
        for key, value in list(self.store.items()):
            if value.valid.is_set():
                yield (key, value.data)

    def validvalues(self):
        for value in list(self.store.values()):
            if value.valid.is_set():
                yield value.data

    def remove_predicate(self, predicate):
        result = []
        for k, v in self.itervalid():
            if predicate(v):
                self.remove(k)
                result.append(k)

        return result

    def query(self, *filter, **params):
        return wrap(list(self.validvalues())).query(*filter, **params)


class EventCacheStore(CacheStore):
    def __init__(self, dispatcher, name):
        super(EventCacheStore, self).__init__()
        self.dispatcher = dispatcher
        self.ready = False
        self.name = name

    def put(self, key, data):
        ret = super(EventCacheStore, self).put(key, data)
        if self.ready:
            self.dispatcher.emit_event('{0}.changed'.format(self.name), {
                'operation': 'create' if ret else 'update',
                'ids': [key]
            })

        return ret

    def remove(self, key):
        ret = super(EventCacheStore, self).remove(key)
        if ret and self.ready:
            self.dispatcher.emit_event('{0}.changed'.format(self.name), {
                'operation': 'delete',
                'ids': [key]
            })

        return ret

    def rename(self, oldkey, newkey):
        with self.lock:
            obj = super(EventCacheStore, self).get(oldkey)
            obj['id'] = newkey
            super(EventCacheStore, self).put(newkey, obj)
            super(EventCacheStore, self).remove(oldkey)

        if self.ready:
            self.dispatcher.emit_event('{0}.changed'.format(self.name), {
                'operation': 'rename',
                'ids': [[oldkey, newkey]]
            })

    def propagate(self, event, callback=None):
        if event['operation'] == 'delete':
            for i in event['ids']:
                self.remove(i)

            return

        if event['operation'] == 'rename':
            for o, i in event['ids']:
                self.rename(o, i)

            return

        if event['operation'] in ('create', 'update'):
            for i in event['entities']:
                obj = callback(i) if callback else i
                if not obj:
                    continue
                self.put(obj['id'], obj)

    def populate(self, collection, callback=None):
        for i in collection:
            obj = callback(i) if callback else i
            if obj is not None:
                self.put(obj['id'], obj)
