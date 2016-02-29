'use strict';

/* App Module */

angular.module('Debugger', [
	'ngRoute',
    'ui.bootstrap'
]).
config(['$routeProvider', function($routeProvider) {
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
