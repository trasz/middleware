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
      when('/apidocs/',
        {
          templateUrl: '../static/partials/apidocs.html',
          controller: APIdocController
        }).
      otherwise({redirectTo: '/'});
}]);
