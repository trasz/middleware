# $FreeBSD$

PORTNAME=	docker-py
PORTVERSION=	1.8.1
CATEGORIES=	devel python
MASTER_SITES=	CHEESESHOP
PKGNAMEPREFIX=	${PYTHON_PKGNAMEPREFIX}

RUN_DEPENDS=	${PYTHON_PKGNAMEPREFIX}websocket-client>0:${PORTSDIR}/www/py-websocket-client

MAINTAINER=	jceel@FreeBSD.org
COMMENT=	Python client for Docker.

USES=		python
USE_PYTHON=	autoplist distutils

.include <bsd.port.mk>
