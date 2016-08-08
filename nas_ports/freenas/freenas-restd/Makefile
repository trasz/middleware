# $FreeBSD$

PORTNAME=	freenas-restd
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}
CATEGORIES=	freenas
VALID_CATEGORIES+= freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	FreeNAS REST API adapter

RUN_DEPENDS=	${PYTHON_PKGNAMEPREFIX}falcon>0:${PORTSDIR}/www/py-falcon \
		${PYTHON_PKGNAMEPREFIX}freenas.dispatcher>0:${PORTSDIR}/freenas/py-freenas.dispatcher

USES=		python
USE_PYTHON=	py3kplist

EXTRACT_ONLY=

NO_BUILD=	yes
MAKE_JOBS_UNSAFE= yes
ALL_TARGET=	obj all
WRKSRC=		/usr/middleware_src/restd

checksum:
	${ECHO_CMD} ${.TARGET} not needed because building direct

fetch:
	:

extract:
	:

do-install:
	${MAKE} -C ${WRKSRC} install STAGEDIR=${STAGEDIR} PREFIX=${PREFIX} PYTHON=${PYTHON_CMD}

post-install:
	${PYTHON_CMD} -m compileall ${STAGEDIR}${PREFIX}/lib/restd
	(cd ${STAGEDIR}${PREFIX}/lib/restd; ${FIND} -E . -type f -regex ".*.pyc?" \
		| ${SED} -e 's,^\./,,g' \
		| ${AWK} '{print length, $$0}' | ${SORT} -rn \
		| ${AWK} '{print "lib/restd/"$$2 }' >> ${TMPPLIST})


.include <bsd.port.mk>
