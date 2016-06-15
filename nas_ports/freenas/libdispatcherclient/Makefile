# $FreeBSD$

PORTNAME=	libdispatcherclient
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}

CATEGORIES=	freenas
VALID_CATEGORIES+= freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	FreeNAS dispatcher client C library

RUN_DEPENDS=	jansson>0:${PORTSDIR}/devel/jansson
BUILD_DEPENDS=	jansson>0:${PORTSDIR}/devel/jansson \
		pkgconf>0:${PORTSDIR}/devel/pkgconf

USES=		cmake
EXTRACT_ONLY=

NO_BUILD=	yes
MAKE_JOBS_UNSAFE= yes
ALL_TARGET=	obj all
WRKSRC=		/usr/middleware_src/dispatcher/client/c

checksum fetch:
	${ECHO_CMD} ${.TARGET} not needed because building direct

.include <bsd.port.mk>
