// DebuggerApp.directive('modalDialog', function() {
//   return {
//     restrict: 'E',
//     scope: {
//       show: '='
//     },
//     replace: true, // Replace with the template below
//     transclude: true, // we want to insert custom content inside the directive
//     link: function(scope, element, attrs) {
//       scope.dialogStyle = {};
//       if (attrs.width)
//         scope.dialogStyle.width = attrs.width;
//       if (attrs.height)
//         scope.dialogStyle.height = attrs.height;
//       scope.hideModal = function() {
//         scope.show = false;
//       };
//     },
//     template: '../static/partials/login.html' // See below
//   };
// });


DebuggerApp.directive('modal', function () {
    return {
      template: '<div class="modal fade">' +
          '<div class="modal-dialog">' +
            '<div class="modal-content">' +
              '<div class="modal-header">' +
                '<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>' +
                '<h4 class="modal-title">{{ title }}</h4>' +
              '</div>' +
              '<div class="modal-body" ng-transclude></div>' +
            '</div>' +
          '</div>' +
        '</div>',
      restrict: 'E',
      transclude: true,
      replace:true,
      scope:true,
      link: function postLink(scope, element, attrs) {
        scope.title = attrs.title;

        scope.$watch(attrs.visible, function(value){
          if(value == true)
            $(element).modal('show');
          else
            $(element).modal('hide');
        });

        $(element).on('shown.bs.modal', function(){
          scope.$apply(function(){
            scope.$parent[attrs.visible] = true;
          });
        });

        $(element).on('hidden.bs.modal', function(){
          scope.$apply(function(){
            scope.$parent[attrs.visible] = false;
          });
        });
      }
    };
  });


DebuggerApp.directive('clearIcon', ['$compile',
 function($compile) {
 return {
     restrict : 'A',
     link : function(scope, elem, attrs) {
         var model = attrs.ngModel;
         var template = '<span ng-click=\"clearText(\'' + model + '\')\" class="clearIcon"             style="display:none;">x</span>';
         elem.parent().append($compile(template)(scope));
         var clearIconToggle = function(toggleParam) {
             if(elem.val().trim().length)
                 elem.next().css("display", "inline");
             else {
                 if(elem.next().css("display") == "inline")
                     elem.next().css("display", "none");
             }
         };
         var clearText = function(clearParam) {
             elem.val('');
             clearIconToggle(clearParam);
         };
         elem.on("focus", function(event) {
             clearIconToggle(model);
         });
         elem.on("keyup", function(event) {
             clearIconToggle(model);
         });
         elem.next().on("click", function(event) {
             elem.val('');
             elem.next().css("display", "none");
         });
     }
 }; }]);
