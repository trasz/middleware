#!/usr/bin/env python
#
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

import argparse
import sys
import os
import threading
import six
import getpass

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('Vte', '2.91')
from gi.repository import Gtk, GLib, Vte
from .repl import Context, MainLoop
from .namespace import EntityNamespace, ItemNamespace
from .output import format_value


DEFAULT_MIDDLEWARE_CONFIGFILE = None
CLI_LOG_DIR = os.getcwd()
if os.environ.get('FREENAS_SYSTEM') == 'YES':
    DEFAULT_MIDDLEWARE_CONFIGFILE = '/usr/local/etc/middleware.conf'
    CLI_LOG_DIR = '/var/tmp'

DEFAULT_CLI_CONFIGFILE = os.path.join(os.getcwd(), '.freenascli.conf')


def run_in_gui_thread(target):
    def fn(*args, **kwargs):
        GLib.idle_add(target, *args, **kwargs)

    return fn


class TaskView(Gtk.EventBox):
    def __init__(self):
        super(TaskView, self).__init__()
        self.context = None
        self.scroll = Gtk.ScrolledWindow()
        self.store = Gtk.ListStore(int, str, str, float)
        self.listview = Gtk.TreeView(self.store)
        self.listview.connect('row-activated', self.on_task_select)
        self.add_column(Gtk.CellRendererText, "Task ID", 0)
        self.add_column(Gtk.CellRendererText, "Task name", 1)
        self.add_column(Gtk.CellRendererText, "Created at", 2)
        self.add_column(Gtk.CellRendererProgress, "Percentage", 3)
        self.scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.scroll.add(self.listview)
        self.add(self.scroll)
        self.set_size_request(-1, 200)
        self.show_all()

    def add_column(self, type, title, index):
        renderer = type()
        column = Gtk.TreeViewColumn(title)
        column.pack_start(renderer, True)
        column.add_attribute(renderer, "value" if type is Gtk.CellRendererProgress else "text", index)
        self.listview.append_column(column)

    def init(self, context):
        self.context = context
        self.context.connection.register_event_handler('entity-subscriber.task.changed', run_in_gui_thread(self.on_task_changed))
        self.context.connection.register_event_handler('task.progress', run_in_gui_thread(self.on_task_progress))
        self.context.connection.call_async('task.query', self.populate_tasks, [
            ('state', '=', 'EXECUTING')
        ])

    def on_task_select(self, tv, path, column):
        self.context.ml.process_and_echo('/task {0}'.format(self.store[path][0]))

    def on_task_changed(self, args):
        if args['operation'] == 'create':
            for i in args['entities']:
                self.store.append([
                    i['id'],
                    i['name'],
                    str(i['created_at']),
                    0
                ])

    def on_task_progress(self, args):
        for i in self.store:
            if i[0] == args['id']:
                i[3] = args['percentage']

    def populate_tasks(self, result):
        for i in result:
            self.store.append([
                i['id'],
                i['name'],
                str(i['created_at']),
                0
            ])


class EntityView(Gtk.EventBox):
    def __init__(self, table):
        super(EntityView, self).__init__()
        self.scroll = Gtk.ScrolledWindow()
        self.store = Gtk.ListStore(object)
        self.listview = Gtk.TreeView(self.store)
        self.scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.scroll.add(self.listview)
        self.add(self.scroll)
        self.set_size_request(-1, 200)
        self.populate(table)
        self.show_all()

    def populate(self, table):
        def data_func(column, cell, model, iter, data=None):
            cell.set_property('text', format_value(model.get_value(iter, 0).get(data.accessor), data.vt))

        for i in table.columns:
            renderer = Gtk.CellRendererText()
            column = Gtk.TreeViewColumn(i.label)
            column.pack_start(renderer, True)
            column.set_cell_data_func(renderer, data_func, i)
            self.listview.append_column(column)

        for i in table.data:
            self.store.append([i])


class ItemView(Gtk.ScrolledWindow):
    def __init__(self, namespace):
        super(ItemView, self).__init__()
        self.table = None
        self.populate(namespace)

    def populate(self, namespace):
        self.table = Gtk.Grid()
        for idx, i in enumerate(namespace.property_mappings):
            label = Gtk.Label(i.descr)
            entry = Gtk.Entry()
            self.table.attach(label, 0, idx, 1, 1)
            self.table.attach_next_to(entry, label, Gtk.PositionType.RIGHT, 1, 1)

        self.add_with_viewport(self.table)
        self.show_all()


class NamespaceView(Gtk.Overlay):
    def __init__(self):
        super(NamespaceView, self).__init__()
        self.progress = Gtk.Spinner()
        self.progress.set_halign(Gtk.Align.CENTER)
        self.progress.set_valign(Gtk.Align.CENTER)
        self.vbox = Gtk.VBox()
        self.scroll = Gtk.ScrolledWindow()
        self.context = None
        self.scroll.add_with_viewport(self.vbox)
        self.add(self.scroll)
        self.add_overlay(self.progress)
        self.set_size_request(200, -1)

    def init(self, context):
        self.context = context
        self.context.on_namespace_enter = self.on_namespace_enter

    def set_overlay(self):
        self.set_sensitive(False)
        self.progress.show()
        self.progress.start()

    def remove_overlay(self):
        self.set_sensitive(True)
        self.progress.stop()
        self.progress.hide()

    def populate(self, up, commands, namespaces):
        def click(button):
            self.set_overlay()
            t = threading.Thread(target=self.context.ml.process_and_echo, args=(button.name,))
            t.setDaemon(True)
            t.start()

        for i in self.vbox.get_children():
            self.vbox.remove(i)

        if up:
            btn = Gtk.Button(label="Go up")
            btn.name = '..'
            btn.connect('clicked', click)
            self.vbox.pack_start(btn, True, True, 0)

        self.vbox.pack_start(Gtk.Label("Namespaces:"), True, True, 0)
        for i in namespaces:
            if isinstance(i, ItemNamespace):
                continue
            btn = Gtk.Button(label=i.name)
            btn.name = i.name
            btn.connect('clicked', click)
            self.vbox.pack_start(btn, True, True, 0)

        self.vbox.pack_start(Gtk.Label("Commands:"), True, True, 0)
        for name in commands:
            btn = Gtk.Button(label=name)
            btn.name = name
            btn.connect('clicked', click)
            self.vbox.pack_start(btn, True, True, 0)

        self.show_all()
        self.remove_overlay()

    def on_namespace_enter(self, namespace):
        up = self.context.ml.cwd is not self.context.root_ns
        commands = list(namespace.commands())
        namespaces = list(namespace.namespaces())
        run_in_gui_thread(self.populate)(up, commands, namespaces)

        if isinstance(namespace, EntityNamespace):
            self.context.ml.process_and_echo('show')

        if isinstance(namespace, ItemNamespace):
            self.context.gui.render_content(ItemView(namespace))


class MainWindow(Gtk.Window):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.hbox = Gtk.HBox()
        self.vbox1 = Gtk.VPaned()
        self.vbox2 = Gtk.VPaned()
        self.evbox = Gtk.EventBox()
        self.content = None
        self.header = Gtk.Label()
        self.terminal = Vte.Terminal()
        self.tasks = TaskView()
        self.namespaces = NamespaceView()
        self.pty = None
        self.vbox1.pack1(self.terminal, True, False)
        self.vbox1.pack2(self.vbox2, True, False)
        self.vbox2.pack1(self.evbox, True, False)
        self.vbox2.pack2(self.tasks, True, False)
        self.hbox.pack_start(self.namespaces, True, True, 0)
        self.hbox.pack_start(self.vbox1, True, True, 0)
        self.add(self.hbox)
        self.context = None
        self.repl_thread = None
        self.connect('delete-event', Gtk.main_quit)

    def render_content(self, content):
        if self.content:
            self.evbox.remove(self.content)

        self.content = content
        self.evbox.add(self.content)

    def render_table(self, table):
        if self.content:
            self.evbox.remove(self.content)

        self.content = EntityView(table)
        self.evbox.add(self.content)

    def start(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('hostname', metavar='HOSTNAME', nargs='?', default='127.0.0.1')
        parser.add_argument('-m', metavar='MIDDLEWARECONFIG', default=DEFAULT_MIDDLEWARE_CONFIGFILE)
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CLI_CONFIGFILE)
        parser.add_argument('-e', metavar='COMMANDS')
        parser.add_argument('-f', metavar='INPUT')
        parser.add_argument('-l', metavar='LOGIN')
        parser.add_argument('-p', metavar='PASSWORD')
        parser.add_argument('-D', metavar='DEFINE', action='append')
        args = parser.parse_args()

        master, slave = os.openpty()
        self.pty = Vte.Pty.new_foreign_sync(master)
        self.terminal.set_pty(self.pty)

        self.context = Context()
        self.context.stdout = os.fdopen(slave, 'w')
        self.context.stdin = os.fdopen(slave, 'r')
        self.context.hostname = args.hostname
        self.context.gui = self
        self.context.read_middleware_config_file(args.m)
        self.context.variables.load(args.c)
        self.context.variables.set('output_format', 'gtk')
        self.context.start()

        if args.hostname not in ('localhost', '127.0.0.1'):
            if args.l is None:
                args.l = six.moves.input('Please provide username: ')
            if args.p is None:
                args.p = getpass.getpass('Please provide password: ')
        if args.l:
            self.context.login(args.l, args.p)
        elif args.l is None and args.p is None and args.hostname in ('127.0.0.1', 'localhost'):
            self.context.login(getpass.getuser(), '')

        ml = MainLoop(self.context)
        self.context.ml = ml
        self.repl_thread = threading.Thread(target=ml.repl)
        self.repl_thread.daemon = True
        self.repl_thread.start()

        self.tasks.init(self.context)
        self.namespaces.init(self.context)
        self.show_all()
        Gtk.main()


if __name__ == '__main__':
    m = MainWindow()
    m.start()
