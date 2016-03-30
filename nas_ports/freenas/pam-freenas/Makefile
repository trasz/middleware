# $FreeBSD$

PORTNAME=	pam-freenas
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}

CATEGORIES=	freenas
VALID_CATEGORIES+= freenas

MAINTAINER=	dev@ixsystems.com
COMMENT=	FreeNAS PAM module

RUN_DEPENDS=	libdispatcherclient>0:${PORTSDIR}/freenas/libdispatcherclient
BUILD_DEPENDS=	libdispatcherclient>0:${PORTSDIR}/freenas/libdispatcherclient

USES=		cmake
EXTRACT_ONLY=

NO_BUILD=	yes
MAKE_JOBS_UNSAFE= yes
ALL_TARGET=	obj all
WRKSRC=		/usr/middleware_src/pam-freenas

checksum fetch:
	${ECHO_CMD} ${.TARGET} not needed because building direct

.include <bsd.port.mk>
