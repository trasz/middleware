#
# $FreeBSD$

PORTNAME=	msock
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}
PKGNAMEPREFIX=	${PYTHON_PKGNAMEPREFIX}

CATEGORIES=	freenas
VALID_CATEGORIES+= freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	Python TCP multiplexing library

USES=		python
USE_PYTHON=	autoplist distutils
EXTRACT_ONLY=

WRKSRC=		/usr/middleware_src/py-msock

checksum fetch:
	${ECHO_CMD} ${.TARGET} not needed because building direct

.include <bsd.port.mk>
