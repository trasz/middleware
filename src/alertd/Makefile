PREFIX ?= /usr/local
PYTHON ?= python

install:
	install etc/alertd ${PREFIX}/etc/rc.d/
	install sbin/alertd ${PREFIX}/sbin/
	install -d ${PREFIX}/lib/alertd
	install -d ${PREFIX}/lib/alertd/src
	install -d ${PREFIX}/lib/alertd/emitters
	cp -a src/ ${PREFIX}/lib/alertd/src/
	cp -a src/ ${PREFIX}/lib/alertd/emitters/
