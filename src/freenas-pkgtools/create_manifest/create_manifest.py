#!/usr/local/bin/python3 -R
import os
import sys
import time
import getopt

sys.path.append("/usr/local/lib")

import freenasOS.Manifest as Manifest
import freenasOS.Configuration as Configuration
from freenasOS.Configuration import ChecksumFile
import freenasOS.Package as Package
import freenasOS.PackageFile as PF


debug = 0
quiet = False
verbose = 0

#
# Create a manifest.  This needs to be given a set of packages,
# a train name, and a sequence number.  If package_directory is
# given, it will search there for the package files.
#
# TBD:  Sequence number should be automatically generated.
#


def usage():
    print("Usage: %s [-P package_directory] [-C configuration_file] [-o output_file] [-N <release_notes_file>] [-R release_name] -T <train_name> -S <manifest_version> pkg=version[:upgrade_from[,...]]  [...]" % sys.argv[0], file=sys.stderr)
    print("\tMultiple -P options allowed; multiple pkg arguments allowed", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    package_dir = None
    searchdirs = []
    notesfile = None
    releasename = None
    trainname = None
    sequencenum = 0
    pkgs = []
    outfile = None
    config_file = None
    timestamp = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "P:C:N:R:T:S:o:t:qvd")
    except getopt.GetoptError as err:
        print(str(err))
        usage()

    for o, a in opts:
        if o == "-P":
            package_dir = a
        elif o == '-C':
            config_file = a
        elif o == "-N":
            notesfile = a
        elif o == "-R":
            releasename = a
        elif o == "-T":
            trainname = a
        elif o == "-S":
            sequence = a
        elif o == "-q":
            quiet = True
        elif o == "-t":
            timestamp = a
        elif o == "-v":
            verbose += 1
        elif o == "-d":
            debug += 1
        elif o == "-o":
            outfile = a
        else:
            usage()

    pkgs = args

    if sequence is None:
        # Use time
        sequence = int(time.time())

    if (trainname is None) or len(pkgs) == 0:
        usage()

    # We need a configuration to do searching
    conf = Configuration.Configuration(file=config_file)
    mani = Manifest.Manifest(conf)
    mani.SetTrain(trainname)
    mani.SetSequence(sequence)
    if timestamp:
        mani.SetTimeStamp(timestamp)

    if releasename is not None:
        mani.SetVersion(releasename)
    if notesfile is not None:
        notes = []
        with open(notesfile, "r") as f:
            for line in f:
                notes.append(line)
        mani.SetNotes(notes)

    if package_dir is not None:
        conf.SetPackageDir(package_dir)

    for P in pkgs:
        if os.path.exists(P):
            # Let's get the manifest from it
            pkgfile = open(P, "rb")
            pkg_json = PF.GetManifest(file=pkgfile)
            name = pkg_json[PF.kPkgNameKey]
            version = pkg_json[PF.kPkgVersionKey]
            hash = ChecksumFile(pkgfile)
            size = os.lstat(P).st_size
            try:
                services = pkg_json[PF.kPkgServicesKey]
            except:
                print("%s is not in pkg_json" % PF.kPkgServicesKey, file=sys.stderr)
                services = None
            try:
                rr = pkg_json[PF.kPkgRebootKey]
            except:
                rr = None

            pkg = Package.Package({
                Package.NAME_KEY: name,
                Package.VERSION_KEY: version,
                Package.CHECKSUM_KEY: hash,
                Package.SIZE_KEY: size,
            })

            if rr is not None:
                print("rr = %s" % rr, file=sys.stderr)
                pkg.SetRequiresReboot(rr)
                if rr is False:
                    # Let's see if we have any services which are restarted by default.
                    print("services = %s" % services, file=sys.stderr)
                    if services and "Restart" in services:
                        # Services has two entries, we want the restart one
                        svcs = services["Restart"]
                        if len(svcs) > 0:
                            print("Restart Services = %s" % list(svcs.keys()), file=sys.stderr)
                            pkg.SetRestartServices(list(svcs.keys()))
        else:
            # This is most likely due to the makefile not being updated, since I
            # changed how the command-line is constructed.  So we'll complain, and
            # then exit abnormally.
            print("I think you did not update the makefile", file=sys.stderr)
            print("%s does not appear to be a valid file" % P, file=sys.stderr)
            sys.exit(1)

        mani.AddPackage(pkg)

    # Don't set the signature
    mani.Validate()

    if outfile is None:
        outfile = sys.stdout
    else:
        outfile = open(outfile, "w")

    print(mani.String(), file=outfile)
