PREFIX ?= /usr/local
PYTHON ?= python

install:
	install etc/debugd ${PREFIX}/etc/rc.d/
	install sbin/debugd ${PREFIX}/sbin/
	install -d ${PREFIX}/lib/debugd
	install -d ${PREFIX}/lib/debugd/src
	cp -a src/ ${PREFIX}/lib/debugd/src/
