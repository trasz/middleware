'use strict';

/* App Module */

angular.module('Debugger', [
	'ngRoute',
]).
config(['$routeProvider', function($routeProvider) {
  $routeProvider.
      when('/',
        {
          templateUrl: '../static/partials/index.html',
          controller: IndexController
        }).
      when('/rpc',{
          templateUrl: '../static/partials/rpc.html',
          controller: RpcController
      }).
      when('/apidocs/',
        {
          templateUrl: '../static/partials/apidocs.html',
          controller: APIdocController
        }).
      otherwise({redirectTo: '/'});
}]);
