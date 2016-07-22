# $FreeBSD$

PORTNAME=	ds2
PORTVERSION=	${PRODUCT_VERSION:C/\-.*//:C/\_.*//}
PORTREVISION=	${REVISION}

CATEGORIES=	devel

MAINTAINER=	wg@FreeBSD.org
COMMENT=	Debug server for lldb

BUILD_DEPENDS=	bison>0:${PORTSDIR}/devel/bison

LICENSE=	NCSA
LICENSE_FILE=	${WRKSRC}/LICENSE

USE_GITHUB=	yes
GH_ACCOUNT=	facebook
GH_TAGNAME=	52c45f3

ONLY_FOR_ARCHS=		amd64
ONLY_FOR_ARCHS_REASON=	the ptrace code only has an amd64 backend

USES=		cmake compiler
EXTRACT_ONLY=

WRKSRC=		/usr/ds2_src

PLIST_FILES=	bin/ds2

checksum fetch:
	${ECHO_CMD} ${.TARGET} not needed because building direct

do-install:
	${INSTALL_PROGRAM} ${WRKSRC}/ds2 ${STAGEDIR}${PREFIX}/bin/

.include <bsd.port.pre.mk>

.if ${OSVERSION} < 1002000 || ${COMPILER_TYPE} == "gcc"
IGNORE=	builds only on greater than 10.1 and with clang
.endif

.include <bsd.port.post.mk>
