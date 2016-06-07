# FreeNAS 10 Middleware

The FreeNAS 10 middleware is a separate process which controls all aspects
of a FreeNAS 10 instance's configuration and management.  It can be talked
to using the CLI (also part of this project) or a web application
(see http://github.com/freenas/gui).

For documentation of the tools and processes for hacking on FreeNAS 10
overall, visit http://doc.freenas.org/10/devdocs.

You should not, however, be working directly in this repo if you simply wish
to build and/or test the middleware.  You should, instead, check out the
http://github.com/freenas/freenas-build project which will automatically
check out all sub-repositories (including this one) and do the right
things to create / update a FreeNAS 10 development instance.

## Setup environment (Mac)
You will need python (>3.4), pip and virtualenv to get your environment setup.  

1. install macports [mac ports installer](https://www.macports.org/install.php)
2. install python3.4
  * sudo port install python34
3. install pip
	* sudo port install py34-pip
4. set active versions of python and pip
	OSX comes with Python2.7 pre-installed.  Use port to select your active version.
	* sudo port select --set python python34
	* sudo port select --set pip pip34
5. use pip to install virtualenv and create symlink if needed (used for build process...docs not complete yet for that...)
	* sudo pip install virtualenv
	* sudo ln -s /usr/local/bin/virtualenv /usr/local/bin/virtualenv-3.4
6. install module dependencies for running cli
	* sudo pip install six paramiko python-dateutil ply jsonschema ws4py gnureadline natural texttable columnize
7. set python path for cli
	* export set PYTHONPATH=../dispatcher/client/python/:../utils
8. change into cli directory and run FreeNAS cli
	* python -m freenas.cli.repl

You should be off to the races at this point.  These docs are a work in progress so please excuse any flaws or omissions.
