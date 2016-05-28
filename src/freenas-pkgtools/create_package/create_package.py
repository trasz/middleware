#!/usr/local/bin/python3 -R
# Create a pkgng-like package from a directory.

import os
import sys
import json
import tarfile
import getopt
import hashlib
import fnmatch
import io
import configparser
import subprocess
import re

debug = 0
verbose = False


# Scan a directory hierarchy, creating a
# "files" and "directories" set of dictionaries.
# Regular files get sha256 checksums.
def ScanTree(root, filter_func=None):
    global debug, verbose
    flat_size = 0
    # This is a list of files we've seen, by <st_dev, st_ino> keys.
    seen_files = {}
    file_list = {}
    directory_list = {}
    for start, dirs, files in os.walk(root):
        prefix = start[len(root):] + "/"
        for d in dirs:
            if filter_func is not None:
                if filter_func(prefix + d) == True:
                    continue
            # This is really bloody annoying.
            # os.walk() uses stat, not lstat.
            # So if this is a symlink, we add it
            # to files.
            if os.path.islink(os.path.join(start, d)):
                files.append(d)
            else:
                directory_list[prefix + d] = "y"
        for f in files:
            if filter_func is not None:
                if filter_func(prefix+f) == True:
                    continue
            full_path = start + "/" + f
            if verbose or debug > 0:
                print("looking at %s" % full_path, file=sys.stderr)
            st = os.lstat(full_path)
            size = None
            if os.path.islink(full_path):
                buf = os.readlink(full_path)
                size = len(buf)
                if buf.startswith("/"):
                    buf = buf[1:]
                file_list[prefix + f] = hashlib.sha256(buf.encode('utf8')).hexdigest()
            elif os.path.isfile(full_path):
                size = st.st_size
                with open(full_path, 'rb') as file:
                    file_list[prefix + f] = hashlib.sha256(file.read()).hexdigest()

            if size is not None and (st.st_dev, st.st_ino) not in seen_files:
                flat_size += size
                seen_files[st.st_dev, st.st_ino] = True

    return {"files": file_list, "directories": directory_list, "flatsize": flat_size}


#
# We need to be told a directory,
# package name, version, and output file.
# We'll assume some defaults specific to ix.

def usage():
    print("Usage: %s [-dv] -R <root> -T template -N <name> -V <version> output_file" % sys.argv[0], file=sys.stderr)
    sys.exit(1)

SCRIPTS = [
    "pre-install",
    "post-install",
    "install",
    "pre-deinstall",
    "post-deinstall",
    "deinstall",
    "pre-upgrade",
    "post-upgrade",
    "upgrade"
]


def ProcessFileList(files, cfg_file):
    cfg_dir = os.path.dirname(cfg_file)
    for f in files:
        f = f.strip()
        if not f.startswith('@'):
            yield f
            continue

        m = re.match(r"@(\w+)\(([^)]*)\)", f)
        if m:
            command, arg = m.groups()
            if command == "include":
                fpath = os.path.join(cfg_dir, arg)
                flist = open(fpath, "r")
                included_files = flist.readlines()
                for i in ProcessFileList(included_files, fpath):
                    yield i

                flist.close()
            else:
                if debug:
                    print("Unknown directive: %s" % (command,), file=sys.stderr)

        else:
            if debug:
                print("Malformed @directive: %s" % (f,), file=sys.stderr)


def TemplateFiles(path):
    """
    Load a ConfigParser file as a configuration file.
    Look for a section labeled "Files", "exclude" and
    "include" otions.  Return both in a dictionary; the
    value is an array of files to include or exclude.
    (The files may be paths, or shell-style globs.)
    """
    rv = {}
    includes = []
    excludes = []
    if os.path.exists(path) == False:
        return None
    if os.path.isdir(path):
        base_dir = path
        cfg_file = path + "/config"
    else:
        base_dir = os.path.dirname(path)
        cfg_file = path

    cfp = configparser.ConfigParser()
    try:
        cfp.read(cfg_file)
    except:
        return None

    if cfp.has_option("Files", "exclude"):
        opt = cfp.get("Files", "exclude")
        for f in opt.split():
            excludes.append(f)

    if cfp.has_option("Files", "include"):
        opt = cfp.get("Files", "include")
        for f in opt.split():
            includes.append(f)

    rv["include"] = list(ProcessFileList(includes, cfg_file))
    rv["exclude"] = list(ProcessFileList(excludes, cfg_file))
    return rv


def LoadTemplate(path):
    """
    Load a ConfigParser file as a template.
    The "Package" section has various defaults for the
    PKGNG-like manifest; other sections have other values
    of interest.
    If path is a directory, then the configuration file
    will be path + "/config".
    For scripts, "file:" indicates a filename to read;
    if path is a directory, it will be relative to that
    directory; otherwise, it will be relative to the directory
    contaning path.  (That is, "/tmp/pkg.cfg" will result in
    "file:+INSTALL" looking for /tmp/+INSTALL.)
    Returns a dictionary, which may be empty.  If path does
    not exist, it raises an exception.
    """
    rv = {}
    if os.path.exists(path) == False:
        raise Exception("%s does not exist" % path)
    if os.path.isdir(path):
        base_dir = path
        cfg_file = path + "/config"
    else:
        base_dir = os.path.dirname(path)
        cfg_file = path

    cfp = configparser.ConfigParser()
    try:
        cfp.read(cfg_file)
    except:
        return rv

    if cfp.has_section("Package"):
        # Get the various manifest settings
        for key in [
            "name", "www", "arch", "maintainer", "comment", "origin",
            "prefix", "licenslogic", "licenses", "desc"
        ]:
            if cfp.has_option("Package", key):
                rv[key] = cfp.get("Package", key)
        # Some optional boolean values
        for key in ["requires-reboot"]:
            if cfp.has_option("Package", key):
                rv[key] = cfp.getboolean("Package", key)

    if cfp.has_section("Scripts"):
        if "scripts" not in rv:
            rv["scripts"] = {}
        for opt, val in cfp.items("Scripts"):
            if val.startswith("file:"):
                # Skip over the file: part
                fname = val[5:]
                if fname.startswith("/") == False:
                    fname = base_dir + "/" + fname
                with open(fname, "r") as f:
                    rv["scripts"][opt] = f.read()
            else:
                rv["scripts"][opt] = val

    # Look for a list of services (and ones to restart)
    if cfp.has_section("Services"):
        service_list = []
        if cfp.has_option("Services", "services"):
            # Great, so it's a comma-seperated list
            service_str = cfp.get("Services", "services")
            for svc in service_str.split(","):
                service_list.append(svc.strip())
        if len(service_list) > 0:
            # Look for services to restart
            restart_list = {}
            if cfp.has_option("Services", "restart"):
                restart_str = cfp.get("Services", "restart")
                if restart_str == "all":
                    for svc in service_list:
                        restart_list[svc] = True
                else:
                    for svc in cfp.get("Services", "restart").split(","):
                        if svc not in service_list:
                            print("Restart service %s not in service list" % svc, file=sys.stderr)
                        else:
                            restart_list[svc] = True
            sdict = {"Services": service_list}
            if len(restart_list) > 0:
                sdict["Restart"] = restart_list
            rv["ix-package-services"] = sdict
    return rv


def main():
    global debug, verbose
    # Some valid, but stupid, defaults.
    manifest = {
        "www": "http://www.freenas.org",
        "arch": "freebsd:10:x86:64",
        "maintainer": "something@freenas.org",
        "comment": "FreeNAS Package",
        "origin": "freenas/os",
        "prefix": "/",
        "licenselogic": "single",
        "desc": "FreeNAS Package",
        "requires-reboot": True,
    }
    root = None
    arg_name = None
    arg_version = None
    arg_template = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "dvN:V:R:T:")
        for o, a in opts:
            if o == "-N":
                arg_name = a
            elif o == "-T":
                arg_template = a
            elif o == "-V":
                arg_version = a
            elif o == "-R":
                root = a
            elif o == "-d":
                debug += 1
            elif o == "-v":
                verbose = True
            else:
                print("Unknown options %s" % o, file=sys.stderr)
                usage()
    except getopt.GetoptError as err:
        print(str(err))
        usage()
    if len(args) > 1:
        print("Too many arguments", file=sys.stderr)
        usage()
    elif len(args) == 0:
        print("Output file must be specified", file=sys.stderr)
        usage()
    else:
        output = args[0]
    if root is None:
        print("Root directory must be specified", file=sys.stderr)
        usage()

    include_list = None
    exclude_list = None
    if arg_template is not None:
        tdict = LoadTemplate(arg_template)
        if tdict is not None:
            for k in list(tdict.keys()):
                manifest[k] = tdict[k]
        print("manifest = %s" % manifest, file=sys.stderr)
        filters = TemplateFiles(arg_template)
        if filters is not None:
            if debug > 1:
                print("Filter list = %s" % filters, file=sys.stderr)
            if len(filters["include"]) > 0:
                include_list = filters["include"]
            if len(filters["exclude"]) > 0:
                exclude_list = filters["exclude"]

    def FilterFunc(path):
        """
        Return a boolean indicating whether the path in question
        should be filtered out.  This is a bit tricky, unfortunately,
        but we'll start out simple.
        Returns True if it should be filtered out, False if not.
        There are four cases to worry about:
        1.  No include_list or exclude_list
        2.  include_list but no exclude_list
        3.  exclude_list but no include_list
        4.  Both include_list and exclude_list
        For (1), return value is always False (never filter out).
        For (2), return value is False if it is in the list, True otherwise.
        For (3), return value is True if it is in the list, False otherwise.
        For (4), return value is True if it is in the exclude list,
        False if it is in the include list, False if it is in neither.
        """
        # Set the default return value based on include and exclude lists.
        if include_list is None and exclude_list is None:
            retval = False
        elif include_list is not None and exclude_list is None:
            retval = True
        elif include_list is None and exclude_list is not None:
            retval = False
        else:
            retval = False

        # Yes, I know, a nested function in a nested function.
        def matches(path, pattern):
            prefix = ""
            if path.startswith("./"):
                prefix = "."
            if pattern.startswith("/"):
                tmp = prefix + pattern
            else:
                tmp = pattern
            # First, check to see if the name simply matches
            if path == tmp:
                if debug:
                    print("Match: %s" % path, file=sys.stderr)
                return True
            # Next, check to see if elem is a subset of it
            if path.startswith(tmp) and \
               path[len(tmp)] == "/":
                if debug:
                    print("Match %s as child of %s" % (path, tmp), file=sys.stderr)
                return True
            # Now to start using globbing.
            # fnmatch is awful, but let's try just that
            # (It's awful because it doesn't handle path boundaries.
            # Thus, "/usr/*.cfg" matches both "/usr/foo.cfg" and
            # "/usr/local/etc/django.cfg".)
            if fnmatch.fnmatch(path, elem):
                if debug:
                    print("Match: %s as glob match for %s" % (path, tmp), file=sys.stderr)
                return True
            return False

        # Include takes precedence over exclude, so we handle
        # the exclude list first
        if exclude_list is not None:
            for elem in exclude_list:
                if matches(path, elem) == True:
                    retval = True
                    break
        if include_list is not None:
            for elem in include_list:
                if matches(path, elem) == True:
                    retval = False
        return retval

    # Command-line versions take precedence over the template
    if arg_name is not None:
        manifest["name"] = arg_name
    if arg_version is not None:
        manifest["version"] = arg_version

    # Now sanity test
    if "name" not in manifest:
        print("Package must have a name", file=sys.stderr)
        print(manifest, file=sys.stderr)
        usage()
    if "version" not in manifest:
        print("Package must have a version", file=sys.stderr)
        usage()

    if debug > 2:
        print(manifest, file=sys.stderr)

    # Now start scanning.
    t = ScanTree(root, FilterFunc)
    manifest["files"] = t["files"]
    manifest["directories"] = t["directories"]
    manifest["flatsize"] = t["flatsize"]
    manifest_string = json.dumps(manifest, sort_keys=True, indent=4, separators=(',', ': '))
    if debug > 1:
        print(manifest_string, file=sys.stderr)

    # I would LOVE to be able to use xz, but python's tarfile does not
    # (as of when I write this) support it.  Python 3 has it.
    temp_file = output.rsplit('.', 1)[0] + '.tar'
    tf = tarfile.open(temp_file, "w", format=tarfile.PAX_FORMAT)

    # Add the manifest string as the file "+MANIFEST"
    mani_file_info = tarfile.TarInfo(name="+MANIFEST")
    mani_file_info.size = len(manifest_string)
    mani_file_info.mode = 0o600
    mani_file_info.type = tarfile.REGTYPE
    mani_file = io.BytesIO(manifest_string.encode('utf8'))
    tf.addfile(mani_file_info, mani_file)
    # Now add all of the files
    for file in sorted(manifest["files"]):
        if verbose or debug > 0:
            print("Adding file %s to archive" % file, file=sys.stderr)
        tf.add(root + file, arcname=file, recursive=False)
    # And now the directories
    for dir in sorted(manifest["directories"]):
        if verbose or debug > 0:
            print("Adding directory %s to archive" % dir, file=sys.stderr)
        tf.add(root + dir, arcname=dir, recursive=False)

    tf.close()

    if os.path.exists('/usr/local/bin/pigz'):
        subprocess.Popen("/usr/local/bin/pigz -c -9 {0} > {1}".format(temp_file, output), shell=True).wait()
    else:
        subprocess.Popen("gzip -c -9 {0} > {1}".format(temp_file, output), shell=True).wait()
    try:
        os.unlink(temp_file)
    except:
        pass

    return 0

if __name__ == "__main__":
    sys.exit(main())
