'use strict';

/* Controllers */

function IndexController($scope) {
  console.log("index page");
}

function RpcController($scope) {
    document.title = "RPC Page";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    $scope.init = function () {
        sock.onError = function(err) {
            alert("Error: " + err.message);
        };
        sock.onConnect = function() {
            if (!sessionStorage.getItem("freenas:username")) {
                var username = prompt("Username:");
                var password = prompt("Password:");
                sessionStorage.setItem("freenas:username", username);
                sessionStorage.setItem("freenas:password", password);
            }

            sock.login(
                sessionStorage.getItem("freenas:username"),
                sessionStorage.getItem("freenas:password")
            );
        };

        sock.onLogin = function() {
            sock.call("discovery.get_services", null, function (services) {
                $scope.$apply(function(){
                    $scope.services = services;
                });
                var service_dict = {};
                $.each(services, function(idx, i) {
                    var temp_list = [];
                    sock.call("discovery.get_methods", [i], function(methods) {
                        console.log(methods);
                        for(var tmp = 0; tmp < methods.length; tmp++) {
                           temp_list.push(methods[tmp]);
                        }
                    service_dict[i] = temp_list;
                      $scope.$apply(function(){
                        $scope.service_dict = service_dict;
                      });
                    });
                });
            });
        };
    }
    $scope.getServiceList = function(service_name){
        $scope.current_methods = $scope.service_dict[service_name];
        $scope.current_service = service_name;
    }
    $scope.setInput = function(method_name) {
      // get method name by ng-click on #service method_list
      // then set value ot #method tag
      // that's all
      clearInputText();
      $("#method").val($("#current_service").html() + "." + method_name);
      setParams();
    }
    function setParams() {
      //fix later
      //temporary solution
      //every time set params to '[]';
      //get params from middleware then set it to #args
      $("#args").val('[]');
    }
    $("#call").click(function () {
        sock.call(
            $("#method").val(),
            JSON.parse($("#args").val()),
            function(result) {
                $("#result").val(JSON.stringify(result, null, 4))
            }
        );
    });
    function clearInputText() {
      //extra tweak for everytime you click on a new method
      //then it should clear all previous text left inside textarea
      $("#method").val('');
      $("#result").val('');
    }
}

function EventsController($scope) {
    document.title = "System Events";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    $scope.init = function () {
        sock.onError = function(err) {
            alert("Error: " + err.message);
        };
        sock.onConnect = function() {
            if (!sessionStorage.getItem("freenas:username")) {
                var username = prompt("Username:");
                var password = prompt("Password:");
                sessionStorage.setItem("freenas:username", username);
                sessionStorage.setItem("freenas:password", password);
            }

            sock.login(
                sessionStorage.getItem("freenas:username"),
                sessionStorage.getItem("freenas:password")
            );
        };
        sock.onLogin = function() {
            sock.subscribe("*");
            console.log("getting system events, plz wait");
            var item_list = [];
            sock.onEvent = function(name, args) {
                var ctx = {
                    name: name,
                    args: JSON.stringify(args, undefined, 4)
                };
                item_list.push(ctx);
                $scope.$apply(function(){
                  $scope.item_list = item_list;
                });
            };
        };
    }
}
function SyslogController($scope) {
    document.title = "System Logs";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    $scope.init = function () {
        sock.onError = function(err) {
            alert("Error: " + err.message);
        };
        sock.onConnect = function() {
            if (!sessionStorage.getItem("freenas:username")) {
                var username = prompt("Username:");
                var password = prompt("Password:");
                sessionStorage.setItem("freenas:username", username);
                sessionStorage.setItem("freenas:password", password);
            }

            sock.login(
                sessionStorage.getItem("freenas:username"),
                sessionStorage.getItem("freenas:password")
            );
        };
        sock.onLogin = function(result) {
            var syslog_list = [];
            sock.call("syslog.query", [[], {"sort": ["-id"], "limit": 50}], function(result) {
                $.each(result, function(idx, i) {
                    syslog_list.push(i);
                });

                sock.registerEventHandler("entity-subscriber.syslog.changed", function(args) {
                    $.each(args.entities, function(idx, i) {
                        syslog_list.push(i);
                    });
                });
                $scope.$apply(function(){
                    $scope.syslog_list = syslog_list;
                });
            });
        };
    }
}

function APIdocController($scope) {
	console.log("api doc page");
}
