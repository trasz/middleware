// var DebuggerServices = angular.module('Debugger', ['ngResource']);
//
// DebuggerServices.factory('')

DebuggerApp.service('synchronousService', [function () {
    var serviceMethod = function (url) {
        var request;
        if (window.XMLHttpRequest) {
            request = new XMLHttpRequest();
        } else if (window.ActiveXObject) {
            request = new ActiveXObject("Microsoft.XMLHTTP");
        } else {
            throw new Error("Your browser don't support XMLHttpRequest");
        }

        request.open('GET', url, false);
        request.send(null);

        if (request.status === 200) {
            return request.responseText;
        }
    };
    return serviceMethod;
}]);
