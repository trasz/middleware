(function(f){if(typeof exports==="object"&&typeof module!=="undefined"){module.exports=f()}else if(typeof define==="function"&&define.amd){define([],f)}else{var g;if(typeof window!=="undefined"){g=window}else if(typeof global!=="undefined"){g=global}else if(typeof self!=="undefined"){g=self}else{g=this}g.middleware = f()}})(function(){var define,module,exports;return (function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
"use strict";

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

Object.defineProperty(exports, "__esModule", {
    value: true
});

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/*
 * Copyright 2015 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

var diff = require('deep-diff').diff;

var CappedMap = (function () {
    function CappedMap(maxsize) {
        _classCallCheck(this, CappedMap);

        this.map = new Map();
        this.maxsize = maxsize;
    }

    _createClass(CappedMap, [{
        key: "get",
        value: function get(key) {
            return this.map.get(key);
        }
    }, {
        key: "set",
        value: function set(key, value) {
            this.map.set(key, value);
            if (this.map.size() > this.maxsize) {
                var mapIter = this.map.entries();
                this.map.delete(mapIter.next().key);
            }
        }
    }, {
        key: "has",
        value: function has(key) {
            return this.map.has(key);
        }
    }, {
        key: "getSize",
        value: function getSize() {
            return this.map.size();
        }
    }, {
        key: "setMaxSize",
        value: function setMaxSize(maxsize) {
            if (maxsize < this.maxsize) {
                var mapIter = this.map.entries();
                var deleteSize = this.map.size() - maxsize;
                for (i = 0; i < deleteSize; i++) {
                    this.map.delete(mapIter.next().key);
                }
            }
            this.maxsize = maxsize;
        }
    }]);

    return CappedMap;
})();

var EntitySubscriber = exports.EntitySubscriber = (function () {
    function EntitySubscriber(client, name) {
        var maxsize = arguments.length <= 2 || arguments[2] === undefined ? 2000 : arguments[2];

        _classCallCheck(this, EntitySubscriber);

        this.name = name;
        this.client = client;
        this.maxsize = maxsize;
        this.objects = new CappedMap(maxsize);
        this.handlerCookie = null;

        /* Callbacks */
        this.onCreate = null;
        this.onUpdate = null;
        this.onDelete = null;
    }

    _createClass(EntitySubscriber, [{
        key: "start",
        value: function start() {
            this.handlerCookie = this.client.registerEventHandler("entity-subscriber." + this.name + ".changed");
        }
    }, {
        key: "stop",
        value: function stop() {
            this.client.unregisterEventHandler(this.handlerCookie);
        }
    }, {
        key: "__onChanged",
        value: function __onChanged(event) {
            if (event.action == "create") {
                var _iteratorNormalCompletion = true;
                var _didIteratorError = false;
                var _iteratorError = undefined;

                try {
                    for (var _iterator = event.entities[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
                        var entity = _step.value;

                        this.objects.set(entity.id, entity);
                    }
                } catch (err) {
                    _didIteratorError = true;
                    _iteratorError = err;
                } finally {
                    try {
                        if (!_iteratorNormalCompletion && _iterator.return) {
                            _iterator.return();
                        }
                    } finally {
                        if (_didIteratorError) {
                            throw _iteratorError;
                        }
                    }
                }
            }

            if (event.action == "update") {
                var _iteratorNormalCompletion2 = true;
                var _didIteratorError2 = false;
                var _iteratorError2 = undefined;

                try {
                    for (var _iterator2 = event.entities[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
                        var entity = _step2.value;

                        this.objects.set(entity.id, entity);
                    }
                } catch (err) {
                    _didIteratorError2 = true;
                    _iteratorError2 = err;
                } finally {
                    try {
                        if (!_iteratorNormalCompletion2 && _iterator2.return) {
                            _iterator2.return();
                        }
                    } finally {
                        if (_didIteratorError2) {
                            throw _iteratorError2;
                        }
                    }
                }
            }

            if (event.action == "delete") {
                var _iteratorNormalCompletion3 = true;
                var _didIteratorError3 = false;
                var _iteratorError3 = undefined;

                try {
                    for (var _iterator3 = event.entities[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
                        var entity = _step3.value;

                        if (this.objects.has(entity.id)) this.objects.delete(entity.id);
                    }
                } catch (err) {
                    _didIteratorError3 = true;
                    _iteratorError3 = err;
                } finally {
                    try {
                        if (!_iteratorNormalCompletion3 && _iterator3.return) {
                            _iterator3.return();
                        }
                    } finally {
                        if (_didIteratorError3) {
                            throw _iteratorError3;
                        }
                    }
                }
            }
        }
    }]);

    return EntitySubscriber;
})();
},{"deep-diff":3}],2:[function(require,module,exports){
"use strict";

var _slicedToArray = (function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; })();

Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.getErrno = getErrno;
exports.getCode = getCode;
/*
 * Copyright 2015 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

var errno = new Map([[1, { "name": "EPERM", "description": "Operation not permitted " }], [2, { "name": "ENOENT", "description": "No such file or directory " }], [3, { "name": "ESRCH", "description": "No such process " }], [4, { "name": "EINTR", "description": "Interrupted system call " }], [5, { "name": "EIO", "description": "Input/output error " }], [6, { "name": "ENXIO", "description": "Device not configured " }], [7, { "name": "E2BIG", "description": "Argument list too long " }], [8, { "name": "ENOEXEC", "description": "Exec format error " }], [9, { "name": "EBADF", "description": "Bad file descriptor " }], [10, { "name": "ECHILD", "description": "No child processes " }], [11, { "name": "EDEADLK", "description": "Resource deadlock avoided " }], [12, { "name": "ENOMEM", "description": "Cannot allocate memory " }], [13, { "name": "EACCES", "description": "Permission denied " }], [14, { "name": "EFAULT", "description": "Bad address " }], [15, { "name": "ENOTBLK", "description": "Block device required " }], [16, { "name": "EBUSY", "description": "Device busy " }], [17, { "name": "EEXIST", "description": "File exists " }], [18, { "name": "EXDEV", "description": "Cross-device link " }], [19, { "name": "ENODEV", "description": "Operation not supported by device " }], [20, { "name": "ENOTDIR", "description": "Not a directory " }], [21, { "name": "EISDIR", "description": "Is a directory " }], [22, { "name": "EINVAL", "description": "Invalid argument " }], [23, { "name": "ENFILE", "description": "Too many open files in system " }], [24, { "name": "EMFILE", "description": "Too many open files " }], [25, { "name": "ENOTTY", "description": "Inappropriate ioctl for device " }], [26, { "name": "ETXTBSY", "description": "Text file busy " }], [27, { "name": "EFBIG", "description": "File too large " }], [28, { "name": "ENOSPC", "description": "No space left on device " }], [29, { "name": "ESPIPE", "description": "Illegal seek " }], [30, { "name": "EROFS", "description": "Read-only filesystem " }], [31, { "name": "EMLINK", "description": "Too many links " }], [32, { "name": "EPIPE", "description": "Broken pipe " }], [33, { "name": "EDOM", "description": "Numerical argument out of domain " }], [34, { "name": "ERANGE", "description": "Result too large " }], [35, { "name": "EAGAIN", "description": "Resource temporarily unavailable " }], [36, { "name": "EINPROGRESS", "description": "Operation now in progress " }], [37, { "name": "EALREADY", "description": "Operation already in progress " }], [38, { "name": "ENOTSOCK", "description": "Socket operation on non-socket " }], [39, { "name": "EDESTADDRREQ", "description": "Destination address required " }], [40, { "name": "EMSGSIZE", "description": "Message too long " }], [41, { "name": "EPROTOTYPE", "description": "Protocol wrong type for socket " }], [42, { "name": "ENOPROTOOPT", "description": "Protocol not available " }], [43, { "name": "EPROTONOSUPPORT", "description": "Protocol not supported " }], [44, { "name": "ESOCKTNOSUPPORT", "description": "Socket type not supported " }], [45, { "name": "EOPNOTSUPP", "description": "Operation not supported " }], [46, { "name": "EPFNOSUPPORT", "description": "Protocol family not supported " }], [47, { "name": "EAFNOSUPPORT", "description": "Address family not supported by protocol family " }], [48, { "name": "EADDRINUSE", "description": "Address already in use " }], [49, { "name": "EADDRNOTAVAIL", "description": "Can't assign requested address " }], [50, { "name": "ENETDOWN", "description": "Network is down " }], [51, { "name": "ENETUNREACH", "description": "Network is unreachable " }], [52, { "name": "ENETRESET", "description": "Network dropped connection on reset " }], [53, { "name": "ECONNABORTED", "description": "Software caused connection abort " }], [54, { "name": "ECONNRESET", "description": "Connection reset by peer " }], [55, { "name": "ENOBUFS", "description": "No buffer space available " }], [56, { "name": "EISCONN", "description": "Socket is already connected " }], [57, { "name": "ENOTCONN", "description": "Socket is not connected " }], [58, { "name": "ESHUTDOWN", "description": "Can't send after socket shutdown " }], [59, { "name": "ETOOMANYREFS", "description": "Too many references: can't splice " }], [60, { "name": "ETIMEDOUT", "description": "Operation timed out " }], [61, { "name": "ECONNREFUSED", "description": "Connection refused " }], [62, { "name": "ELOOP", "description": "Too many levels of symbolic links " }], [63, { "name": "ENAMETOOLONG", "description": "File name too long " }], [64, { "name": "EHOSTDOWN", "description": "Host is down " }], [65, { "name": "EHOSTUNREACH", "description": "No route to host " }], [66, { "name": "ENOTEMPTY", "description": "Directory not empty " }], [67, { "name": "EPROCLIM", "description": "Too many processes " }], [68, { "name": "EUSERS", "description": "Too many users " }], [69, { "name": "EDQUOT", "description": "Disc quota exceeded " }], [70, { "name": "ESTALE", "description": "Stale NFS file handle " }], [71, { "name": "EREMOTE", "description": "Too many levels of remote in path " }], [72, { "name": "EBADRPC", "description": "RPC struct is bad " }], [73, { "name": "ERPCMISMATCH", "description": "RPC version wrong " }], [74, { "name": "EPROGUNAVAIL", "description": "RPC prog. not avail " }], [75, { "name": "EPROGMISMATCH", "description": "Program version wrong " }], [76, { "name": "EPROCUNAVAIL", "description": "Bad procedure for program " }], [77, { "name": "ENOLCK", "description": "No locks available " }], [78, { "name": "ENOSYS", "description": "Function not implemented " }], [79, { "name": "EFTYPE", "description": "Inappropriate file type or format " }], [80, { "name": "EAUTH", "description": "Authentication error " }], [81, { "name": "ENEEDAUTH", "description": "Need authenticator " }], [82, { "name": "EIDRM", "description": "Identifier removed " }], [83, { "name": "ENOMSG", "description": "No message of desired type " }], [84, { "name": "EOVERFLOW", "description": "Value too large to be stored in data type " }], [85, { "name": "ECANCELED", "description": "Operation canceled " }], [86, { "name": "EILSEQ", "description": "Illegal byte sequence " }], [87, { "name": "ENOATTR", "description": "Attribute not found " }], [88, { "name": "EDOOFUS", "description": "Programming error " }], [89, { "name": "EBADMSG", "description": "Bad message " }], [90, { "name": "EMULTIHOP", "description": "Multihop attempted " }], [91, { "name": "ENOLINK", "description": "Link has been severed " }], [92, { "name": "EPROTO", "description": "Protocol error " }], [93, { "name": "ENOTCAPABLE", "description": "Capabilities insufficient " }], [94, { "name": "ECAPMODE", "description": "Not permitted in capability mode " }], [95, { "name": "ENOTRECOVERABLE", "description": "State not recoverable " }], [96, { "name": "EOWNERDEAD", "description": "Previous owner died " }]]);

function getErrno(code) {
    return errno.get(code);
}

function getCode(name) {
    var _iteratorNormalCompletion = true;
    var _didIteratorError = false;
    var _iteratorError = undefined;

    try {
        for (var _iterator = errno.entries()[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
            var _step$value = _slicedToArray(_step.value, 2);

            var k = _step$value[0];
            var v = _step$value[1];

            if (v.name == name) return {
                "code": k,
                "name": v.name,
                "description": v.description
            };
        }
    } catch (err) {
        _didIteratorError = true;
        _iteratorError = err;
    } finally {
        try {
            if (!_iteratorNormalCompletion && _iterator.return) {
                _iterator.return();
            }
        } finally {
            if (_didIteratorError) {
                throw _iteratorError;
            }
        }
    }

    return null;
}
},{}],3:[function(require,module,exports){
(function (global){
/*!
 * deep-diff.
 * Licensed under the MIT License.
 */
;(function(root, factory) {
  'use strict';
  if (typeof define === 'function' && define.amd) {
    // AMD. Register as an anonymous module.
    define([], factory);
  } else if (typeof exports === 'object') {
    // Node. Does not work with strict CommonJS, but
    // only CommonJS-like environments that support module.exports,
    // like Node.
    module.exports = factory();
  } else {
    // Browser globals (root is window)
    root.DeepDiff = factory();
  }
}(this, function(undefined) {
  'use strict';

  var $scope, conflict, conflictResolution = [];
  if (typeof global === 'object' && global) {
    $scope = global;
  } else if (typeof window !== 'undefined') {
    $scope = window;
  } else {
    $scope = {};
  }
  conflict = $scope.DeepDiff;
  if (conflict) {
    conflictResolution.push(
      function() {
        if ('undefined' !== typeof conflict && $scope.DeepDiff === accumulateDiff) {
          $scope.DeepDiff = conflict;
          conflict = undefined;
        }
      });
  }

  // nodejs compatible on server side and in the browser.
  function inherits(ctor, superCtor) {
    ctor.super_ = superCtor;
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  }

  function Diff(kind, path) {
    Object.defineProperty(this, 'kind', {
      value: kind,
      enumerable: true
    });
    if (path && path.length) {
      Object.defineProperty(this, 'path', {
        value: path,
        enumerable: true
      });
    }
  }

  function DiffEdit(path, origin, value) {
    DiffEdit.super_.call(this, 'E', path);
    Object.defineProperty(this, 'lhs', {
      value: origin,
      enumerable: true
    });
    Object.defineProperty(this, 'rhs', {
      value: value,
      enumerable: true
    });
  }
  inherits(DiffEdit, Diff);

  function DiffNew(path, value) {
    DiffNew.super_.call(this, 'N', path);
    Object.defineProperty(this, 'rhs', {
      value: value,
      enumerable: true
    });
  }
  inherits(DiffNew, Diff);

  function DiffDeleted(path, value) {
    DiffDeleted.super_.call(this, 'D', path);
    Object.defineProperty(this, 'lhs', {
      value: value,
      enumerable: true
    });
  }
  inherits(DiffDeleted, Diff);

  function DiffArray(path, index, item) {
    DiffArray.super_.call(this, 'A', path);
    Object.defineProperty(this, 'index', {
      value: index,
      enumerable: true
    });
    Object.defineProperty(this, 'item', {
      value: item,
      enumerable: true
    });
  }
  inherits(DiffArray, Diff);

  function arrayRemove(arr, from, to) {
    var rest = arr.slice((to || from) + 1 || arr.length);
    arr.length = from < 0 ? arr.length + from : from;
    arr.push.apply(arr, rest);
    return arr;
  }

  function realTypeOf(subject) {
    var type = typeof subject;
    if (type !== 'object') {
      return type;
    }

    if (subject === Math) {
      return 'math';
    } else if (subject === null) {
      return 'null';
    } else if (Array.isArray(subject)) {
      return 'array';
    } else if (subject instanceof Date) {
      return 'date';
    } else if (/^\/.*\//.test(subject.toString())) {
      return 'regexp';
    }
    return 'object';
  }

  function deepDiff(lhs, rhs, changes, prefilter, path, key, stack) {
    path = path || [];
    var currentPath = path.slice(0);
    if (typeof key !== 'undefined') {
      if (prefilter && prefilter(currentPath, key, { lhs: lhs, rhs: rhs })) {
        return;
      }
      currentPath.push(key);
    }
    var ltype = typeof lhs;
    var rtype = typeof rhs;
    if (ltype === 'undefined') {
      if (rtype !== 'undefined') {
        changes(new DiffNew(currentPath, rhs));
      }
    } else if (rtype === 'undefined') {
      changes(new DiffDeleted(currentPath, lhs));
    } else if (realTypeOf(lhs) !== realTypeOf(rhs)) {
      changes(new DiffEdit(currentPath, lhs, rhs));
    } else if (lhs instanceof Date && rhs instanceof Date && ((lhs - rhs) !== 0)) {
      changes(new DiffEdit(currentPath, lhs, rhs));
    } else if (ltype === 'object' && lhs !== null && rhs !== null) {
      stack = stack || [];
      if (stack.indexOf(lhs) < 0) {
        stack.push(lhs);
        if (Array.isArray(lhs)) {
          var i, len = lhs.length;
          for (i = 0; i < lhs.length; i++) {
            if (i >= rhs.length) {
              changes(new DiffArray(currentPath, i, new DiffDeleted(undefined, lhs[i])));
            } else {
              deepDiff(lhs[i], rhs[i], changes, prefilter, currentPath, i, stack);
            }
          }
          while (i < rhs.length) {
            changes(new DiffArray(currentPath, i, new DiffNew(undefined, rhs[i++])));
          }
        } else {
          var akeys = Object.keys(lhs);
          var pkeys = Object.keys(rhs);
          akeys.forEach(function(k, i) {
            var other = pkeys.indexOf(k);
            if (other >= 0) {
              deepDiff(lhs[k], rhs[k], changes, prefilter, currentPath, k, stack);
              pkeys = arrayRemove(pkeys, other);
            } else {
              deepDiff(lhs[k], undefined, changes, prefilter, currentPath, k, stack);
            }
          });
          pkeys.forEach(function(k) {
            deepDiff(undefined, rhs[k], changes, prefilter, currentPath, k, stack);
          });
        }
        stack.length = stack.length - 1;
      }
    } else if (lhs !== rhs) {
      if (!(ltype === 'number' && isNaN(lhs) && isNaN(rhs))) {
        changes(new DiffEdit(currentPath, lhs, rhs));
      }
    }
  }

  function accumulateDiff(lhs, rhs, prefilter, accum) {
    accum = accum || [];
    deepDiff(lhs, rhs,
      function(diff) {
        if (diff) {
          accum.push(diff);
        }
      },
      prefilter);
    return (accum.length) ? accum : undefined;
  }

  function applyArrayChange(arr, index, change) {
    if (change.path && change.path.length) {
      var it = arr[index],
        i, u = change.path.length - 1;
      for (i = 0; i < u; i++) {
        it = it[change.path[i]];
      }
      switch (change.kind) {
        case 'A':
          applyArrayChange(it[change.path[i]], change.index, change.item);
          break;
        case 'D':
          delete it[change.path[i]];
          break;
        case 'E':
        case 'N':
          it[change.path[i]] = change.rhs;
          break;
      }
    } else {
      switch (change.kind) {
        case 'A':
          applyArrayChange(arr[index], change.index, change.item);
          break;
        case 'D':
          arr = arrayRemove(arr, index);
          break;
        case 'E':
        case 'N':
          arr[index] = change.rhs;
          break;
      }
    }
    return arr;
  }

  function applyChange(target, source, change) {
    if (target && source && change && change.kind) {
      var it = target,
        i = -1,
        last = change.path ? change.path.length - 1 : 0;
      while (++i < last) {
        if (typeof it[change.path[i]] === 'undefined') {
          it[change.path[i]] = (typeof change.path[i] === 'number') ? [] : {};
        }
        it = it[change.path[i]];
      }
      switch (change.kind) {
        case 'A':
          applyArrayChange(change.path ? it[change.path[i]] : it, change.index, change.item);
          break;
        case 'D':
          delete it[change.path[i]];
          break;
        case 'E':
        case 'N':
          it[change.path[i]] = change.rhs;
          break;
      }
    }
  }

  function revertArrayChange(arr, index, change) {
    if (change.path && change.path.length) {
      // the structure of the object at the index has changed...
      var it = arr[index],
        i, u = change.path.length - 1;
      for (i = 0; i < u; i++) {
        it = it[change.path[i]];
      }
      switch (change.kind) {
        case 'A':
          revertArrayChange(it[change.path[i]], change.index, change.item);
          break;
        case 'D':
          it[change.path[i]] = change.lhs;
          break;
        case 'E':
          it[change.path[i]] = change.lhs;
          break;
        case 'N':
          delete it[change.path[i]];
          break;
      }
    } else {
      // the array item is different...
      switch (change.kind) {
        case 'A':
          revertArrayChange(arr[index], change.index, change.item);
          break;
        case 'D':
          arr[index] = change.lhs;
          break;
        case 'E':
          arr[index] = change.lhs;
          break;
        case 'N':
          arr = arrayRemove(arr, index);
          break;
      }
    }
    return arr;
  }

  function revertChange(target, source, change) {
    if (target && source && change && change.kind) {
      var it = target,
        i, u;
      u = change.path.length - 1;
      for (i = 0; i < u; i++) {
        if (typeof it[change.path[i]] === 'undefined') {
          it[change.path[i]] = {};
        }
        it = it[change.path[i]];
      }
      switch (change.kind) {
        case 'A':
          // Array was modified...
          // it will be an array...
          revertArrayChange(it[change.path[i]], change.index, change.item);
          break;
        case 'D':
          // Item was deleted...
          it[change.path[i]] = change.lhs;
          break;
        case 'E':
          // Item was edited...
          it[change.path[i]] = change.lhs;
          break;
        case 'N':
          // Item is new...
          delete it[change.path[i]];
          break;
      }
    }
  }

  function applyDiff(target, source, filter) {
    if (target && source) {
      var onChange = function(change) {
        if (!filter || filter(target, source, change)) {
          applyChange(target, source, change);
        }
      };
      deepDiff(target, source, onChange);
    }
  }

  Object.defineProperties(accumulateDiff, {

    diff: {
      value: accumulateDiff,
      enumerable: true
    },
    observableDiff: {
      value: deepDiff,
      enumerable: true
    },
    applyDiff: {
      value: applyDiff,
      enumerable: true
    },
    applyChange: {
      value: applyChange,
      enumerable: true
    },
    revertChange: {
      value: revertChange,
      enumerable: true
    },
    isConflict: {
      value: function() {
        return 'undefined' !== typeof conflict;
      },
      enumerable: true
    },
    noConflict: {
      value: function() {
        if (conflictResolution) {
          conflictResolution.forEach(function(it) {
            it();
          });
          conflictResolution = null;
        }
        return accumulateDiff;
      },
      enumerable: true
    }
  });

  return accumulateDiff;
}));

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],"dispatcher-client":[function(require,module,exports){
'use strict';

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.DispatcherClient = exports.RPCException = exports.getCode = exports.getErrno = exports.EntitySubscriber = undefined;

var _EntitySubscriber = require('./EntitySubscriber.js');

Object.defineProperty(exports, 'EntitySubscriber', {
    enumerable: true,
    get: function get() {
        return _EntitySubscriber.EntitySubscriber;
    }
});

var _ErrnoCodes = require('./ErrnoCodes.js');

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/*
 * Copyright 2015 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

var INVALID_JSON_RESPONSE = 1;
var CONNECTION_TIMEOUT = 2;
var CONNECTION_CLOSED = 3;
var RPC_CALL_TIMEOUT = 4;
var RPC_CALL_ERROR = 5;
var SPURIOUS_RPC_RESPONSE = 6;
var LOGOUT = 7;
var OTHER = 8;

exports.getErrno = _ErrnoCodes.getErrno;
exports.getCode = _ErrnoCodes.getCode;

var RPCException = exports.RPCException = function RPCException(code, message) {
    var extra = arguments.length <= 2 || arguments[2] === undefined ? null : arguments[2];
    var stacktrace = arguments.length <= 3 || arguments[3] === undefined ? null : arguments[3];

    _classCallCheck(this, RPCException);

    this.code = code;
    this.message = message;
    this.extra = extra;
    this.stacktrace = stacktrace;
};

var DispatcherClient = exports.DispatcherClient = (function () {
    function DispatcherClient(hostname) {
        _classCallCheck(this, DispatcherClient);

        this.defaultTimeout = 20;
        this.hostname = hostname;
        this.socket = null;
        this.pendingCalls = new Map();
        this.eventHandlers = new Map();

        /* Callbacks */
        this.onEvent = function () {};
        this.onConnect = function () {};
        this.onDisconnect = function () {};
        this.onLogin = function () {};
        this.onRPCResponse = function () {};
        this.onError = function () {};
    }

    _createClass(DispatcherClient, [{
        key: '__onmessage',
        value: function __onmessage(msg) {
            try {
                var data = JSON.parse(msg.data);
            } catch (e) {
                console.warn('Malformed response: "' + msg.data + '"');
                return;
            }

            if (data.namespace == "events" && data.name == "event") {
                this.onEvent(data.args);
                return;
            }

            if (data.namespace == "events" && data.name == "logout") {
                this.onError(LOGOUT);
            }

            if (data.namespace == "rpc") {
                if (data.name == "call") {
                    console.error("Server functionality is not supported");
                    this.onError(SPURIOUS_RPC_RESPONSE);
                }

                if (data.name == "response") {
                    if (!this.pendingCalls.has(data.id)) {
                        console.warn('Spurious RPC response: ' + data.id);
                        this.onError(SPURIOUS_RPC_RESPONSE);
                        return;
                    }

                    var call = this.pendingCalls.get(data.id);
                    clearTimeout(call.timeout);
                    call.callback(data.args);
                    this.pendingCalls.delete(data.id);
                }
            }
        }
    }, {
        key: '__onopen',
        value: function __onopen() {
            console.log("Connection established");
            this.onConnect();
        }
    }, {
        key: '__onclose',
        value: function __onclose() {
            console.log("Connection closed");
            this.onDisconnect();
        }
    }, {
        key: '__ontimeout',
        value: function __ontimeout(id) {
            var call = this.pendingCalls.get(id);
            var errno = (0, _ErrnoCodes.getCode)("ETIMEDOUT");

            call.callback(new RPCException(errno.code, errno.description));

            this.pendingCalls.delete(id);
        }
    }, {
        key: 'connect',
        value: function connect() {
            this.socket = new WebSocket('ws://' + this.hostname + ':5000/socket');
            this.socket.onmessage = this.__onmessage.bind(this);
            this.socket.onopen = this.__onopen.bind(this);
            this.socket.onclose = this.__onclose.bind(this);
        }
    }, {
        key: 'login',
        value: function login(username, password) {
            var _this = this;

            var id = DispatcherClient.__uuid();
            var payload = {
                "username": username,
                "password": password
            };

            this.pendingCalls.set(id, {
                "callback": function callback() {
                    return _this.onLogin();
                }
            });

            this.socket.send(DispatcherClient.__pack("rpc", "auth", payload, id));
        }
    }, {
        key: 'call',
        value: function call(method, args, callback) {
            var _this2 = this;

            var id = DispatcherClient.__uuid();
            var timeout = setTimeout(function () {
                _this2.__ontimeout(id);
            }, this.defaultTimeout * 1000);

            var payload = {
                "method": method,
                "args": args
            };

            this.pendingCalls.set(id, {
                "method": method,
                "args": args,
                "callback": callback,
                "timeout": timeout
            });

            this.socket.send(DispatcherClient.__pack("rpc", "call", payload, id));
        }
    }, {
        key: 'emitEvent',
        value: function emitEvent(name, args) {
            this.socket.send(DispatcherClient.__pack("events", "event", {
                "name": name,
                "args": "args"
            }, null));
        }
    }, {
        key: 'registerEventHandler',
        value: function registerEventHandler(name, callback) {
            if (!this.eventHandlers.has(name)) {
                this.eventHandlers.set(name, new Map());
            }

            var cookie = DispatcherClient.__uuid();
            var list = this.eventHandlers.get(name);
            list.set(cookie, callback);
        }
    }, {
        key: 'unregisterEventHandler',
        value: function unregisterEventHandler(cookie) {}
    }], [{
        key: '__uuid',
        value: function __uuid() {
            return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0,
                    v = c == "x" ? r : r & 0x3 | 0x8;
                return v.toString(16);
            });
        }
    }, {
        key: '__pack',
        value: function __pack(namespace, name, args, id) {
            return JSON.stringify({
                "namespace": namespace,
                "id": id || DispatcherClient.__uuid(),
                "name": name,
                "args": args
            });
        }
    }]);

    return DispatcherClient;
})();
},{"./EntitySubscriber.js":1,"./ErrnoCodes.js":2}]},{},[])("dispatcher-client")
});