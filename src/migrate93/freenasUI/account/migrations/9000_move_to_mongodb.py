# -*- coding: utf-8 -*-
import os
import uuid
from south.utils import datetime_utils as datetime
from south.db import db
from south.v2 import DataMigration
from django.db import models

from datastore import get_datastore


NOGROUP_ID = '8980c534-6a71-4bfb-bc72-54cbd5a186db'


def bsdusr_sshpubkey(user):
    keysfile = '%s/.ssh/authorized_keys' % user.bsdusr_home
    if not os.path.exists(keysfile):
        return ''
    try:
        with open(keysfile, 'r') as f:
            keys = f.read()
        return keys
    except:
        return None


def convert_smbhash(obj, smbhash):
    if not smbhash:
        obj.update({
            'nthash': None,
            'lmhash': None,
            'password_changed_at': None
        })
        return obj

    try:
        pieces = smbhash.strip().split(':')
        lmhash = pieces[2]
        nthash = pieces[3]
        lct = int(pieces[5].split('-')[1], 16)
    except:
        lmhash = None
        nthash = None
        lct = 0

    obj.update({
        'lmhash': lmhash,
        'nthash': nthash,
        'password_changed_at': datetime.fromtimestamp(lct)
    })
    return obj


class Migration(DataMigration):
    def forwards(self, orm):
        # Skip for install time, we only care for upgrades here
        if 'FREENAS_INSTALL' in os.environ:
            return

        ds = get_datastore()

        for g in orm['account.bsdGroups'].objects.filter(bsdgrp_builtin=False):
            ds.insert('groups', {
                'id': str(uuid.uuid4()),
                'gid': g.bsdgrp_gid,
                'name': g.bsdgrp_group,
                'bultin': False,
                'sudo': g.bsdgrp_sudo,
            })

        for u in orm['account.bsdUsers'].objects.all():
            groups = []
            for bgm in orm['account.bsdGroupMembership'].objects.filter(bsdgrpmember_user=u):
                grp = ds.query('groups', ('gid', '=', bgm.bsdgrpmember_group.bsdgrp_gid))
                if not grp:
                    continue

                groups.append(grp['id'])

            if u.bsdusr_builtin:
                continue

            grp = ds.query('groups', ('gid', '=', u.bsdusr_group.bsdgrp_gid))
            user = {
                'id': str(uuid.uuid4()),
                'uid': u.bsdusr_uid,
                'username': u.bsdusr_username,
                'unixhash': u.bsdusr_unixhash,
                'group': grp['id'] if grp else NOGROUP_ID,
                'home': u.bsdusr_home,
                'shell': u.bsdusr_shell,
                'full_name': u.bsdusr_full_name,
                'builtin': False,
                'email': u.bsdusr_email,
                'password_disabled': u.bsdusr_password_disabled,
                'locked': u.bsdusr_locked,
                'sudo': u.bsdusr_sudo,
                'sshpubkey': bsdusr_sshpubkey(u),
                'groups': groups,
            }

            convert_smbhash(user,  u.bsdusr_smbhash)
            ds.insert('users', user)

        ds.collection_record_migration('groups', 'freenas9_migration')
        ds.collection_record_migration('users', 'freenas9_migration')

    def backwards(self, orm):
        "Write your backwards methods here."

    models = {
        u'account.bsdgroupmembership': {
            'Meta': {'object_name': 'bsdGroupMembership'},
            'bsdgrpmember_group': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['account.bsdGroups']"}),
            'bsdgrpmember_user': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['account.bsdUsers']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'account.bsdgroups': {
            'Meta': {'ordering': "['bsdgrp_builtin', 'bsdgrp_group']", 'object_name': 'bsdGroups'},
            'bsdgrp_builtin': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'bsdgrp_gid': ('django.db.models.fields.IntegerField', [], {}),
            'bsdgrp_group': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '120'}),
            'bsdgrp_sudo': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'account.bsdusers': {
            'Meta': {'ordering': "['bsdusr_builtin', 'bsdusr_username']", 'object_name': 'bsdUsers'},
            'bsdusr_builtin': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'bsdusr_email': ('django.db.models.fields.EmailField', [], {'max_length': '75', 'blank': 'True'}),
            'bsdusr_full_name': ('django.db.models.fields.CharField', [], {'max_length': '120'}),
            'bsdusr_group': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['account.bsdGroups']"}),
            'bsdusr_home': ('freenasUI.freeadmin.models.fields.PathField', [], {'default': "'/nonexistent'", 'max_length': '255'}),
            'bsdusr_locked': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'bsdusr_password_disabled': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'bsdusr_shell': ('django.db.models.fields.CharField', [], {'default': "'/bin/csh'", 'max_length': '120'}),
            'bsdusr_smbhash': ('django.db.models.fields.CharField', [], {'default': "'*'", 'max_length': '128', 'blank': 'True'}),
            'bsdusr_sudo': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'bsdusr_uid': ('django.db.models.fields.IntegerField', [], {}),
            'bsdusr_unixhash': ('django.db.models.fields.CharField', [], {'default': "'*'", 'max_length': '128', 'blank': 'True'}),
            'bsdusr_username': ('django.db.models.fields.CharField', [], {'default': "u'User &'", 'unique': 'True', 'max_length': '16'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        }
    }

    complete_apps = ['account']
    symmetrical = True
