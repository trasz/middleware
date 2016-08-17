PREFIX ?= /usr/local
PYTHON ?= python

install:
	install etc/debugd ${PREFIX}/etc/rc.d/
	install etc/debugd.conf ${PREFIX}/etc/
	install sbin/debugd ${PREFIX}/sbin/
	install sbin/debugctl ${PREFIX}/sbin/
	install -d ${PREFIX}/lib/debugd
	install -d ${PREFIX}/lib/debugd/src
	cp -a src/ ${PREFIX}/lib/debugd/src/
