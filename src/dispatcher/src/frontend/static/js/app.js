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
    //   when('/vm',{
    //       templateUrl: '../static/partials/vm.html',
    //       controller: VMController
    //   }).
      when('/filebrowser',{
          templateUrl: '../static/partials/filebrowser.html' ,
          controller: FileBrowserController
      }).
      when('/apidocs/rpc', {
          templateUrl: '../static/partials/apidoc_rpc.html',
          controller: RPCdocController
        }).
      when('/apidocs/tasks', {
          templateUrl: '../static/partials/apidoc_tasks.html',
          controller: TaskDocController
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
      when('/topsecret', {
          templateUrl: '../static/partials/apidocs.html',
          controller: AprilFoolController
      }).
      otherwise({redirectTo: '/'});
}]);
