PREFIX ?= /usr/local
PYTHON ?= python

install:
	install etc/restd ${STAGEDIR}${PREFIX}/etc/rc.d/
	install tools/restd ${STAGEDIR}${PREFIX}/sbin/
	install -d ${STAGEDIR}${PREFIX}/lib/restd/src
	cp -R restd/ ${STAGEDIR}${PREFIX}/lib/dispatcher/src/
