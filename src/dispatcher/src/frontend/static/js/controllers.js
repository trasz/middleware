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

        console.log("init func");
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
                        for(var tmp = 0; tmp < methods.length; tmp++) {
                           temp_list.push(methods[tmp].name);
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
        console.log(service_name + " :  " + $scope.service_dict[service_name]);
        $scope.current_methods = $scope.service_dict[service_name];
        $scope.current_service = service_name;
    }
    $scope.setInput = function(method_name) {
      // get method name by ng-click on #service method_list
      // then set value ot #method tag
      // that's all
      clearInputText();
      $("#method").val(method_name);
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


function APIdocController($scope) {
	console.log("api doc page");
}
