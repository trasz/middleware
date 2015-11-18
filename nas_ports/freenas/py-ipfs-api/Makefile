# $FreeBSD$
# $Id$
#

PORTNAME=       ipfs-api
PORTVERSION=    ${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=   ${REVISION}
PKGNAMEPREFIX=  ${PYTHON_PKGNAMEPREFIX}

CATEGORIES=     freenas
VALID_CATEGORIES+= freenas

MAINTAINER=     dev@ixsystems.com
COMMENT=        IPFS API Bindings for Python

RUN_DEPENDS=  	ipfs-go:${PORTSDIR}/sysutils/ipfs-go \
				${PYTHON_PKGNAMEPREFIX}requests>=2.2.1:${PORTSDIR}/www/py-requests \
				${PYTHON_PKGNAMEPREFIX}six>0:${PORTSDIR}/devel/py-six

USES=			python
EXTRACT_ONLY=


WRKSRC=		/usr/middleware_src/py-ipfs-api

USES=		python
USE_PYTHON=	autoplist distutils

checksum fetch:
	echo ${.TARGET} not needed because building direct

.include <bsd.port.mk>
