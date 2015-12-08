PREFIX ?= /usr/local
PYTHON ?= python

fetch:
	mkdir -p rancher
	fetch -o rancher/vmlinuz http://releases.rancher.com/os/latest/vmlinuz
	fetch -o rancher/initrd http://releases.rancher.com/os/latest/initrd

install:
	install etc/containerd ${PREFIX}/etc/rc.d/
	install sbin/containerd ${PREFIX}/sbin/
	install -d ${PREFIX}/lib/containerd
	install -d ${PREFIX}/lib/containerd/src
	install -d ${PREFIX}/lib/containerd/plugins
	install -d ${PREFIX}/share/containerd
	install -d ${PREFIX}/share/containerd/rancheros
	cp -a rancher/ ${PREFIX}/share/containerd/rancheros/
	cp -a src/ ${PREFIX}/lib/containerd/src/
