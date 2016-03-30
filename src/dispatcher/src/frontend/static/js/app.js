'use strict';

/* App Module */

var DebuggerApp = angular.module('Debugger', [
	'ngRoute',
    'ui.bootstrap',
]);
DebuggerApp.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
      when('/',
        {
            templateUrl: '../static/partials/rpc.html',
            controller: RpcController
        }).
      when('/rpc',{
          templateUrl: '../static/partials/rpc.html',
          controller: RpcController
      }).
      when('/term',{
          templateUrl: '../static/partials/term.html' ,
          controller: TermController
      }).
	  when('/events',{
		  templateUrl: '../static/partials/events.html',
		  controller: EventsController
	  }).
      when('/syslog',{
          templateUrl: '../static/partials/syslog.html',
          controller: SyslogController
      }).
      when('/stats', {
          templateUrl: '../static/partials/stats.html',
          controller: StatsController
      }).
      when('/tasks', {
          templateUrl: '../static/partials/tasks.html' ,
          controller: TasksController
      }).
      when('/filebrowser',{
          templateUrl: '../static/partials/filebrowser.html' ,
          controller: FileBrowserController
      }).
      when('/apidocs/', {
          templateUrl: '../static/partials/apidocs.html',
          controller: APIdocController
        }).
      when('/404',{
          templateUrl: '../static/partials/404.html',
          controller: Four04Controller
      }).
      when('/500', {
          templateUrl: '../static/partials/500.html',
          controller: Five00Controller
      }).
      when('/status/:status_code/',{
          templateUrl: '../static/partials/status.html',
          controller: HTTPStatusController
      }).
      otherwise({redirectTo: '/'});
}]);

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

DebuggerApp.directive('ngRightClick', function($parse) {
    return function(scope, element, attrs) {
        var fn = $parse(attrs.ngRightClick);
        element.bind('contextmenu', function(event) {
            scope.$apply(function() {
                event.preventDefault();
                fn(scope, {$event:event});
            });
        });
    };
});
