#
# $FreeBSD$

PORTNAME=	dockerhub
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}
PKGNAMEPREFIX=	${PYTHON_PKGNAMEPREFIX}

CATEGORIES=	freenas
VALID_CATEGORIES+= freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	Python Docker Hub API library

RUN_DEPENDS=	${PYTHON_PKGNAMEPREFIX}requests>0:${PORTSDIR}/www/py-requests

USES=		python
USE_PYTHON=	autoplist distutils
EXTRACT_ONLY=

WRKSRC=		/usr/middleware_src/py-dockerhub

MAKE_ENV=	FREEBSD_SRC=/usr/src

checksum fetch:
	${ECHO_CMD} ${.TARGET} not needed because building direct

.include <bsd.port.mk>
