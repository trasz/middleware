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


import time
import copy
import uuid
import dateutil.parser
from datetime import datetime
from pymongo import MongoClient
import pymongo
import pymongo.errors
import pymongo.cursor
from six import string_types
from datastore import DatastoreException, DuplicateKeyException
from freenas.utils.query import get


def auto_retry(fn):
    def wrapped(*args, **kwargs):
        for i in range(0, 15):
            self = args[0]
            if not self.connected:
                time.sleep(1)
                continue

            try:
                return fn(*args, **kwargs)
            except (pymongo.errors.AutoReconnect, pymongo.errors.ConnectionFailure, pymongo.errors.OperationFailure):
                time.sleep(1)

        raise DatastoreException('Cannot connect to MongoDB instance')

    return wrapped


class MongodbDatastore(object):
    def __init__(self):
        self.conn_db = None
        self.conn_log = None
        self.db = None
        self.log_db = None
        self.connected = False
        self.operators_table = {
            '>': '$gt',
            '<': '$lt',
            '>=': '$gte',
            '<=': '$lte',
            '!=': '$ne',
            'in': '$in',
            'nin': '$nin',
            '~': '$regex',
        }

        self.clauses_table = {
            'or': lambda v: {'$or': [self._predicate(*t) for t in v]},
            'nor': lambda v: {'$nor': [self._predicate(*t) for t in v]},
            'and': lambda v: {'$and': [self._predicate(*t) for t in v]},
            'where': lambda v: {'$where': v},
            'text': lambda v: {'$text': {'$search': v, '$language': 'none'}}
        }

        self.conversions_table = {
            'timestamp': lambda v: dateutil.parser.parse(v)
        }

    @property
    def client(self):
        return self.conn_db

    @property
    def client_log(self):
        return self.conn_log

    def _predicate(self, *args):
        if len(args) == 2:
            return self._joint_predicate(*args)

        if len(args) in (3, 4):
            return self._operator_predicate(*args)

    def _operator_predicate(self, name, op, value, conversion=None):
        if name == 'id':
            name = '_id'

        if conversion:
            value = self.conversions_table[conversion](value)

        if op == '=':
            return {name: value}

        if op in self.operators_table:
            if op in ('in', 'nin'):
                if isinstance(value, (list, tuple)):
                    return {name: {self.operators_table[op]: value}}
                else:
                    return {name: {self.operators_table[op]: [value]}}

            return {name: {self.operators_table[op]: value}}

    def _joint_predicate(self, op, value):
        if op in self.clauses_table:
            return self.clauses_table[op](value)

    def _build_query(self, params):
        result = []
        for item in params:
            r = self._predicate(*item)
            if r:
                result.append(r)

        return {'$and': result} if len(result) > 0 else {}

    def _get_db(self, collection):
        c = self.db['collections'].find_one({"_id": collection})
        if not c:
            raise DatastoreException('Collection {0} not found'.format(collection))

        typ = c['attributes'].get('type', 'config')

        if typ == 'log':
            return self.log_db[collection]

        return self.db[collection]

    def connect(self, dsn, dsn_log, database='freenas'):
        self.conn_db = MongoClient(dsn)
        self.db = self.conn_db[database]

        if dsn_log:
            self.conn_log = MongoClient(dsn_log, connect=False)
            self.log_db = self.conn_log[database]

        self.connected = True

    def close(self):
        self.conn_db.close()
        if self.conn_log:
            self.conn_log.close()

    @auto_retry
    def collection_create(self, name, pkey_type='uuid', attributes=None):
        attributes = attributes or {}
        ttl_index = attributes.get('ttl_index')
        unique_indexes = attributes.get('unique_indexes', [])
        cap = attributes.get('cap')

        if not self.db['collections'].find_one(name):
            self.db['collections'].insert({
                '_id': name,
                'pkey-type': pkey_type,
                'attributes': attributes
            })

        db = self._get_db(name).database

        if name not in db.collection_names():
            db.create_collection(name)

        if cap:
            db.command('convertToCapped', name, size=cap)

        if ttl_index:
            self.db[name].create_index(ttl_index, expireAfterSeconds=0)

        for idx in unique_indexes:
            if isinstance(idx, str):
                idx = [idx]

            self.db[name].create_index([(i, pymongo.ASCENDING) for i in idx], unique_indexes=True)

        self.db[name].create_index([('$**', pymongo.TEXT)])

    @auto_retry
    def collection_exists(self, name):
        return self.db['collections'].find_one({"_id": name}) is not None

    @auto_retry
    def collection_get_attrs(self, name):
        item = self.db['collections'].find_one({"_id": name})
        return item['attributes']

    @auto_retry
    def collection_set_attrs(self, name):
        item = self.db['collections'].find_one({"_id": name})
        return item['attributes']

    @auto_retry
    def collection_get_migration_policy(self, name):
        item = self.db['collections'].find_one({"_id": name})
        return item.get('migration', 'keep')

    @auto_retry
    def collection_get_migrations(self, name):
        item = self.db['collections'].find_one({"_id": name})
        return item.get('migrations', [])

    @auto_retry
    def collection_has_migration(self, name, migration_name):
        item = self.db['collections'].find_one({"_id": name})
        return migration_name in item.get('migrations', [])

    @auto_retry
    def collection_record_migration(self, name, migration_name):
        item = self.db['collections'].find_one({"_id": name})
        migs = item.setdefault('migrations', [])
        migs.append(migration_name)
        self.db['collections'].update({'_id': name}, item)

    @auto_retry
    def collection_list(self):
        return [x['_id'] for x in self.db['collections'].find()]

    @auto_retry
    def collection_delete(self, name):
        if not self.db['collections'].find_one({"_id": name}):
            return

        self._get_db(name).drop()
        self.db['collections'].remove({'_id': name})

    @auto_retry
    def collection_get_pkey_type(self, name):
        item = self.db['collections'].find_one({"_id": name})
        return item['pkey-type']

    @auto_retry
    def collection_set_pkey_type(self, name, type):
        item = self.db['collections'].find_one({"_id": name})
        item['pkey-type'] = type
        self.db['collections'].update({'_id': name}, item)

    @auto_retry
    def collection_get_next_pkey(self, name, prefix):
        counter = 0
        while True:
            pkey = prefix + str(counter)
            if not self.exists(name, ('id', '=', pkey)):
                return pkey

            counter += 1

    @auto_retry
    def query(self, collection, *args, **kwargs):
        single = kwargs.get('single', False)
        count = kwargs.get('count', False)

        if single or count:
            return self.query_stream(collection, *args, **kwargs)

        return list(self.query_stream(collection, *args, **kwargs))

    @auto_retry
    def query_stream(self, collection, *args, **kwargs):
        sort = kwargs.pop('sort', None)
        limit = kwargs.pop('limit', None)
        offset = kwargs.pop('offset', None)
        single = kwargs.pop('single', False)
        count = kwargs.pop('count', False)
        postprocess = kwargs.pop('callback', None)
        select = kwargs.pop('select', None)

        db = self._get_db(collection)
        cur = db.find(self._build_query(args))
        if count:
            return cur.count()

        if select:
            def select_fn(fn, obj):
                obj = fn(obj) if fn else obj

                if isinstance(select, (list, tuple)):
                    return [get(obj, i) for i in select]

                if isinstance(select, str):
                    return get(obj, select)

            old = postprocess
            postprocess = lambda o: select_fn(old, o)

        if sort:
            def sort_transform(result, key):
                direction = pymongo.ASCENDING
                if key.startswith('-'):
                    key = key[1:]
                    direction = pymongo.DESCENDING
                key = '_id' if key == 'id' else key
                _sort.append((key, direction))

            _sort = []
            if isinstance(sort, string_types):
                sort_transform(_sort, sort)
            elif isinstance(sort, (tuple, list)):
                for s in sort:
                    sort_transform(_sort, s)
            if _sort:
                cur = cur.sort(_sort)

        if offset:
            cur = cur.skip(offset)

        if limit:
            cur = cur.limit(limit)

        if single:
            i = next(cur, None)
            if i is None:
                return i

            i['id'] = i.pop('_id')
            return postprocess(i) if postprocess else i

        def gen():
            for i in cur:
                i['id'] = i.pop('_id')
                r = postprocess(i) if postprocess else i
                if r is not None:
                    yield r

        return gen()

    @auto_retry
    def listen(self, collection, *args, **kwargs):
        return self._get_db(collection).find(
            self._build_query(args),
            cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT
        )

    @auto_retry
    def tail(self, cur):
        try:
            for i in cur:
                i['id'] = i.pop('_id')
                yield i
        except (pymongo.errors.OperationFailure, pymongo.errors.AutoReconnect) as err:
            raise DatastoreException(str(err))

    @auto_retry
    def get_one(self, collection, *args, **kwargs):
        db = self._get_db(collection)
        obj = db.find_one(self._build_query(args))
        if obj is None:
            return None

        obj['id'] = obj.pop('_id')
        return obj

    @auto_retry
    def get_by_id(self, collection, pkey):
        db = self._get_db(collection)
        obj = db.find_one({'_id': pkey})
        if obj is None:
            return None

        obj['id'] = obj.pop('_id')
        return obj

    @auto_retry
    def exists(self, collection, *args, **kwargs):
        return self.get_one(collection, *args, **kwargs) is not None

    @auto_retry
    def insert(self, collection, obj, pkey=None, timestamp=True, config=False):
        if hasattr(obj, '__getstate__'):
            obj = obj.__getstate__()
        elif type(obj) is not dict or config:
            obj = {'value': obj}
        else:
            obj = copy.copy(obj)

        pkey_type = self.collection_get_pkey_type(collection)
        autopkey = pkey is None and 'id' not in obj
        retries = 100

        if 'id' in obj:
            pkey = obj.pop('id')

        if pkey_type == 'uuid' and pkey:
            pkey = pkey.lower()

        while True:
            if autopkey:
                if pkey_type in ('serial', 'integer'):
                    ret = self._get_db(collection).find_one(sort=[('_id', pymongo.DESCENDING)])
                    pkey = ret['_id'] + 1 if ret else 1
                elif pkey_type == 'uuid':
                    pkey = str(uuid.uuid4())

            obj['_id'] = pkey
            if timestamp:
                t = datetime.utcnow()
                obj['updated_at'] = t
                obj['created_at'] = t

            try:
                db = self._get_db(collection)
                db.insert(obj)
            except pymongo.errors.DuplicateKeyError:
                if autopkey and retries > 0:
                    retries -= 1
                    continue

                raise DuplicateKeyException('Document with given key already exists')

            return pkey

    @auto_retry
    def update(self, collection, pkey, obj, upsert=False, timestamp=True, config=False):
        if hasattr(obj, '__getstate__'):
            obj = obj.__getstate__()
        elif type(obj) is not dict or config:
            obj = {'value': obj}
        else:
            obj = copy.deepcopy(obj)

        if 'id' in obj and pkey != obj['id']:
            # We gonna remove the document and reinsert it to change the id...
            full_obj = self.get_by_id(collection, pkey)
            full_obj.update(obj)
            self.delete(collection, pkey)
            self.insert(collection, full_obj, pkey=obj['id'], timestamp=False)
            return

        if 'id' in obj:
            del obj['id']

        if timestamp:
            t = datetime.utcnow()
            obj['updated_at'] = t

            if not self.get_by_id(collection, pkey):
                obj['created_at'] = t

        db = self._get_db(collection)
        db.update({'_id': pkey}, obj, upsert=upsert)

    def upsert(self, collection, pkey, obj, config=False):
        return self.update(collection, pkey, obj, upsert=True, config=config)

    @auto_retry
    def delete(self, collection, pkey):
        db = self._get_db(collection)
        db.remove(pkey)

    def lock(self, data=True, log=False):
        if data:
            self.conn_db.fsync(lock=True)

        if log:
            self.conn_log.fsync(lock=True)

    def unlock(self, data=True, log=False):
        if data:
            self.conn_db.unlock()

        if log:
            self.conn_log.unlock()
