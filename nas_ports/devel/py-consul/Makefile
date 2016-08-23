# $FreeBSD$

PORTNAME=	python-consul
PORTVERSION=	0.6.0
CATEGORIES=	devel python
MASTER_SITES=	CHEESESHOP
PKGNAMEPREFIX=	${PYTHON_PKGNAMEPREFIX}

RUN_DEPENDS=	${PYTHON_PKGNAMEPREFIX}six>1.4.0:${PORTSDIR}/devel/py-six \
		${PYTHON_PKGNAMEPREFIX}requests>2.0.0:${PORTSDIR}/www/py-requests

MAINTAINER=	dev@ixsystems.com
COMMENT=	Python client for Consul.io.

USES=		python
USE_PYTHON=	autoplist distutils

.include <bsd.port.mk>
