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
    $("#result").hide();
    $scope.init = function () {
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
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
                console.log(result);
                // if (result.length == undefined) {
                    // result['extra'] = "<a href='' ng-click='setInput(" + result['extra']+ ")'>" + result['extra'] + "</a>";
                // }else {
                // $.each(result, function(idx, i) {
                    // console.log(i);
                    // there're 3 conditions : [], [object, object], RPCException
                    // Now I got every single object from socket,
                    // should add some check like doHaveRef(),
                    // then add ref_link for `$ref`
                    // use angular.toJson(obj, pretty);
                    // and $compile(HTML)(scope);
                // });
                // }
                $("#result").html(angular.toJson(result, 4));
                $("#result").show("slow");
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

function TermController($scope, synchronousService) {
    console.log("200");
    document.title = "System Events";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    function connect_term(client, command){
        var conn = new middleware.ShellClient(client);
        conn.connect(command);
        conn.onOpen = function() {
            var term = new Terminal({
                cols: 80,
                rows: 24,
                screenKeys: true
            });

            term.on('data', function (data) {
                conn.send(data);
            });

            conn.onData = function (data) {
                term.write(data);
            };

            term.open($("#terminal")[0])
            $scope.$apply(function(){
                $scope.term = term;
            });
        }
    }
    $scope.init = function () {
        var syncUrl = "/static/term.js";
        synchronousService(syncUrl);
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
        };
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
        $("#login_username").html(username);
    };
    sock.onLogin = function() {
        sock.call("shell.get_shells", null, function(response) {
            var dataSource_list = [];
            $.each(response, function(idx, i) {
                dataSource_list.push(i);
            });
            $scope.$apply(function(){
                $scope.dataSource_list = dataSource_list;
            });
        });

        connect_term(sock, "/bin/sh")
    };
    $scope.loadShell = function(source_name) {
        $("#terminal").html("");
        connect_term(sock, source_name);
    }

}

function EventsController($scope) {
    document.title = "System Events";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    $scope.init = function () {
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
        };
        sock.onLogin = function() {
            sock.subscribe("*");
            console.log("getting system events, plz wait");
            var item_list = [];
            sock.onEvent = function(name, args) {
                var ctx = {
                    name: name,
                    args: angular.toJson(args, 4)
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
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
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
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
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

function FileBrowserController($scope) {
    document.title = "File Browser";
    var BUFSIZE = 1024;
    var sock = new middleware.DispatcherClient( document.domain );
    sock.connect();
    $scope.init = function () {
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
        };

        sock.onConnect = function ( ) {
          if ( !sessionStorage.getItem( "freenas:username" ) ) {
            var username = prompt( "Username:" );
            var password = prompt( "Password:" );
            sessionStorage.setItem( "freenas:username", username );
            sessionStorage.setItem( "freenas:password", password );
          }

          sock.login
          ( sessionStorage.getItem( "freenas:username" )
          , sessionStorage.getItem( "freenas:password" )
          );

          $("#login_username").html(username);
        };

        sock.onLogin = function ( ) {
          listDir( "/root" );
        };
    }
    // Utility Helper functions
    function pathJoin ( parts, sep ) {
      var separator = sep || "/";
      var replace   = new RegExp( separator + "{1,}", "g" );
      return parts.join( separator ).replace( replace, separator );
    }

    function humanFileSize ( bytes, si ) {
      var thresh = si ? 1000 : 1024;
      if ( Math.abs( bytes ) < thresh ) {
        return bytes + " B";
      }
      var units = si
          ? [ "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" ]
          : [ "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB" ];
      var u = -1;
      do {
        bytes /= thresh;
        ++u;
      } while ( Math.abs( bytes ) >= thresh && u < units.length - 1 );
      return bytes.toFixed( 1 ) + " " + units[u];
    };
    // Utility Helper functions

    function humanizeDirItem(item) {
        var date = new Date( 0 );
        item.modified = new Date( date.setUTCSeconds( item.modified ) );
        item.size = humanFileSize( item.size, false );
        return item
    }

    function sendBlob ( fileconn, file, optStartByte, optStopByte ) {
      var start = parseInt( optStartByte ) || 0;
      var stop = parseInt( optStopByte ) || file.size;

      var reader = new FileReader();

      reader.onloadend = function ( evt ) {
        if ( evt.target.readyState == FileReader.DONE ) { // DONE == 2
          console.log
            ( "readBlob byte_range: Read bytes: "
            , start
            , " - "
            , stop
            , " of "
            , file.size
            , " byte file"
          );
          fileconn.send( evt.target.result );
          if ( stop == file.size ) {
            // we are done with the transfer, AWESOME!
            // disconnet after a small delay
            setTimeout( function ( ) {
                fileconn.disconnect();
              }, 2000 );
          }
        }
      };

      var blob = file.slice( start, stop );
      reader.readAsArrayBuffer( blob );
    }

    function uploadToSocket ( file ) {
      console.log( "uploadToSocket: Initializing FileClient now" );
      var fileconn = new middleware.FileClient( sock );
      fileconn.onOpen = function ( ) {
        console.log( "FileConnection opened, Websocket resdyState: ", fileconn.socket.readyState );
        var filePos = 0;
        while ( filePos + BUFSIZE <= file.size ) {
          sendBlob( fileconn, file, filePos, filePos + BUFSIZE );
          filePos = filePos + BUFSIZE;
        }
        if ( filePos < file.size ) {
          sendBlob( fileconn, file, filePos, file.size );
        }
      };
      fileconn.onData = function ( msg ) {
        console.log( "FileConnection message recieved is ", msg );
      };
      fileconn.onClose = function ( ) {
        console.log( "FileConnection closed" );
      };
      fileconn.upload(
          pathJoin(
            [ sessionStorage.getItem( "filebrowser:cwd" ), file.name ]
          )
        , file.size
        , "777"
      );

    }

    var listDir = function ( path, relative ) {
      if ( relative === true ) {
        path = pathJoin( [ sessionStorage.getItem( "filebrowser:cwd" ), path ] );
      }
      if ( path === "" ) { path = "/"; }
      sock.call( "filesystem.list_dir", [ path ], function ( dirs ) {
        $( "#dirlist tbody" ).empty();
        $( "#cwd" ).html( "Current Path: " + path );
        $( "#cdup" ).on( "click", function ( e ) {
          if ( path !== "/" ) {
            listDir( path.substring( 0, path.lastIndexOf( "/" ) ) );
          };
        });
        sessionStorage.setItem( "filebrowser:cwd", path );
        // $scope.current_dirs = dirs.map(humanizeDirItem);
        $scope.$apply(function(){
            $scope.current_dir_items = dirs.map(humanizeDirItem);
        });
      });

      $scope.uploadFiles = function() {
          console.log("this is where we upload files");
          $scope.uploadFileList.map(uploadToSocket);
      }

      $scope.browseFolder = function(foldername) {
          listDir(foldername, true);
      }

      $scope.downloadFile = function(filename) {
          //downloadFromHttp(filename);
          downloadFromHttp( filename );
        //   toggleMenuOff();
        // console.log(filename);
      }

      function handleFileSelect ( evt ) {
        evt.stopPropagation();
        evt.preventDefault();

        var files = evt.dataTransfer.files; // FileList object.

        $( "#outputfilelist" ).empty();
        console.log(files);
        $scope.uploadFileList = [];
        $.each( files, function ( key, file ) {
          var date = file.lastModifiedDate ? file.lastModifiedDate.toLocaleDateString() : "n/a";
            $scope.uploadFileList.push(file);
        });
        $scope.$apply(function(){
            $scope.hasFileSelected = true;
            $scope.uploadFileList;
        });
      }

      function handleDragOver ( evt ) {
        evt.stopPropagation();
        evt.preventDefault();
        evt.dataTransfer.dropEffect = "copy"; // Explicitly show this is a copy.
      }

      function downloadFromHttp ( filename ) {
        console.log( "downloadFromHttp: Starting download of file: ", filename );
        var path = pathJoin(
              [ sessionStorage.getItem( "filebrowser:cwd" ), filename ]
          );
        var fileconn = new middleware.FileClient( sock );
        fileconn.download ( path, filename, "static" );
      }

      function downloadFromSocket ( filename ) {
        console.log( "downloadFromSocket: Initializing FileClient now" );
        var path = pathJoin(
              [ sessionStorage.getItem( "filebrowser:cwd" ), filename ]
          );
        fileconn = new middleware.FileClient( sock );
        fileconn.onOpen = function ( ) {
          console.log( "FileConnection opened, Websocket resdyState: ", fileconn.socket.readyState );
        };
        fileconn.onData = function ( msg ) {
          console.log( "FileConnection message recieved is ", msg );
        };
        fileconn.onClose = function ( ) {
          console.log( "FileConnection closed" );
        };
        fileconn.download( path, filename, "stream" );
      }

      // Setup the dnd listeners.
      var dropZone = document.getElementById( "drop_zone" );
      dropZone.addEventListener( "dragover", handleDragOver, false );
      dropZone.addEventListener( "drop", handleFileSelect, false );

    };
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
    $scope.init = function() {
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
        };
        sock.onLogin = function() {
            sock.subscribe("task.*");
            refresh_tasks();
            var item_list = [];
            sock.call("discovery.get_tasks", null, function (tasks) {
                $.each(tasks, function(key, value) {
                    value['name'] = key;
                    value['schema'] = angular.toJson(value['schema'], 4);
                    item_list.push(value);
                });
                $scope.$apply(function(){
                  $scope.item_list = item_list;
                });
            });
        }
    }
}

function VMController($scope) {
    var sock = new middleware.DispatcherClient(document.domain);
    var term;
    var conn;
    var currentId = null;
    sock.connect();

    function connect_term(client, vm) {
        conn = new middleware.ContainerConsoleClient(client);
        conn.connect(vm);
        conn.onOpen = function() {
            term = new Terminal({
                cols: 80,
                rows: 24,
                screenKeys: true
            });

            term.on('data', function (data) {
                conn.send(data);
            });

            conn.onData = function (data) {
                term.write(data);
            };

            term.open($("#terminal")[0])
        }
    }
    $scope.init = function() {
        var syncUrl = "/static/term.js";
        synchronousService(syncUrl);
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
    }
    $("#containers").on("click", "a.container-entry", function() {
        sock.call("containerd.management.get_status", [$(this).attr("data-id")], function (response) {
            $("#state").text("State: " + response.state);
        });

        if (term) {
            term.destroy();
            conn.disconnect();
        }

        currentId = $(this).attr("data-id");
        connect_term(sock, currentId);
    });
    $("#start-container").on("click", function() {
        sock.call("task.submit", ["container.start", [currentId]]);
    });

    $("#stop-container").on("click", function() {
        sock.call("task.submit", ["container.stop", [currentId]]);
    });
}

function Four04Controller($scope) {

}

function Five00Controller($scope) {

}

function HTTPStatusController($scope, $http, $routeParams, $location) {
    $scope.status_code = $routeParams.status_code;
}

function RPCdocController($scope) {
    document.title = "RPC API Page";
    var sock = new middleware.DispatcherClient(document.domain);
    sock.connect();
    $scope.init = function() {
        $(".json").each(function() {
            $(this).JSONView($(this).text(), { "collapsed": true });
            $(this).JSONView('expand', 1);
        });
        sock.onError = function(err) {
            $("#socket_status ").attr("src", "/static/images/service_issue_diamond.png");
            $("#refresh_page_glyph").show();
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
            $("#login_username").html(username);
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
}
