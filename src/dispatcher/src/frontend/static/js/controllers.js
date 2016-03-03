'use strict';
restrict: 'A';

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
            if(typeof err.message != 'undefined'){
                alert("Error :" + err.message);
            }else{
                alert("Connection closed, refresh me");
            }
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
                // $.each(result, function(idx, i) {
                    // console.log(i);
                    // Now I got every single object from socket,
                    // should add some check like doHaveRef(),
                    // then add ref_link for `$ref`
                // });
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

function StatsController($scope) {
    document.title = "Stats Charts";
    var sock = new middleware.DispatcherClient(document.domain);
    var chart;
    sock.connect();
    function render_chart(data){
        chart = c3.generate({
            bindto: "#chart",
            data: {
                x: "x",
                rows: [["x", "value"]].concat(data)
            },
            color: {
                pattern: ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c', '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5', '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f', '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']
            },
            axis: {
                x: {
                    type: "timeseries",
                    tick: {
                        format: function(x) {
                            return moment.unix(x).format('MMM Do, HH:mm:ss');
                        }
                    }
                }
            }
        })
    }

    function update_chart(event){
        chart.flow({
            rows: [["x", "value"], [event.timestamp, event.value]]
        })
    }

    function load_chart(name){
        $("#title").text(name);
        sock.subscribe("statd." + name + ".pulse");
        sock.call("statd.output.query", [name, {
            start: moment().subtract($("#timespan").val(), "minutes").format(),
            end: moment().format(),
            frequency: $("#frequency").val()
        }], function (response) {
            render_chart(response.data);
        });
    }
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
        //onLogin function do everyting you need to render a chart
        sock.onLogin = function() {
            sock.onEvent = function(name, args) {
                if (name == "statd." + $("#title").text() + ".pulse")
                    update_chart(args);
            };

            sock.call("statd.output.get_data_sources", [], function(response) {
                var dataSource_list = [];
                $.each(response, function(idx, i) {
                    dataSource_list.push(i);
                });
                $scope.$apply(function(){
                    $scope.dataSource_list = dataSource_list;
                });
            });
        };
        $scope.loadSource = function(source_name) {
                load_chart(source_name);
        }

        $("#call").click(function() {
            load_chart($("#title").text())
        })
    }
}

function TasksController($scope) {
    document.title = "System Tasks";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    function refresh_tasks(){
        $("#tasklist tbody").empty();
        sock.call("task.query", [[["state", "in", ["CREATED", "WAITING", "EXECUTING"]]]], function (tasks) {
            $.each(tasks, function(idx, i) {
                $("<tr/>", {
                    'data-id': i.id,
                    'html': template_task(i)
                }).appendTo("#tasklist tbody");
            });
        });
    }
    $scope.init() = function() {
        sock.onError = function(err) {
            alert("Error: " + err.message);
        };
        sock.onEvent = function(name, args) {
            if (name == "task.created") {
                $("<tr/>", {
                    'data-id': args.id,
                    'html': template_task(args)
                }).appendTo("#tasklist tbody");
            }

            if (name == "task.updated") {
                var tr = $("#tasklist").find("tr[data-id='" + args.id + "']");
                tr.find(".status").text(args.state);
            }

            if (name == "task.progress") {
                var tr = $("#tasklist").find("tr[data-id='" + args.id + "']");
                tr.find(".progress .progress-bar").css("width", args.percentage.toFixed(2) + "%");
                tr.find(".progress .progress-bar").text(args.percentage.toFixed() + "%");
                tr.find(".message").text(args.message);
            }
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
            sock.subscribe("task.*");
            refresh_tasks();
            sock.call("discovery.get_tasks", null, function (tasks) {
                $.each(tasks, function(key, value) {
                    $("<div/>", {
                        "class": "panel panel-primary",
                        style: "width: 40%",
                        html: template_class({name: key, args: value})
                    }).prependTo("#tasks");
                });
            });
        }
    }
}

function Four04Controller($scope) {

}

function Five00Controller($scope) {

}

function HTTPStatusController($scope, $http, $routeParams, $location) {
    $scope.status_code = $routeParams.status_code;
}

function APIdocController($scope) {
	console.log("api doc page");
}
