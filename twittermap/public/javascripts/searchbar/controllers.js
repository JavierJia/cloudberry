angular.module('cloudberry.util', ['rzModule', 'cloudberry.common'])
  .controller('SearchCtrl', function($scope, $window, cloudberry, cloudberryConfig) {
    $scope.search = function() {
      var type = $scope.mode;
      var interval = $scope.interval.value;
      var keywords = [];
      if ($scope.keyword && $scope.keyword.trim().length > 0) {
        keywords = $scope.keyword.trim().split(/\s+/);
      }

      var parameters = {
        type: type,
        interval: interval,
        keywords: keywords
      };
      cloudberry.query(parameters);
    };
    $scope.predefinedKeywords = cloudberryConfig.predefinedKeywords;
    $scope.updateSearchBox = function (keyword) {
      $('.search-keyword-btn').html(keyword + ' <span class="caret"></span>');
    };
    $scope.predefinedSearch = function (keyword) {
      $scope.keyword = keyword;
      $scope.search();
      $scope.updateSearchBox(keyword);
    };
    $scope.interval = {
              value : 24,
              options: {
                floor: 1,
                ceil: 1600,
                translate: function(num) {
                  return num + 'h';
                }
              }
    };
    $scope.modeType = {
      equalResult : "equal-result",
      equalResponse: "equal-response",
      minBackup : "min-backups"
    };
    $scope.mode = $scope.modeType.equalResult;
    $scope.$on("slideEnded", function() {
        cloudberry.updateInterval($scope.mode, $scope.interval.value);
    });
    $scope.$watch(
        function () {
           return $scope.mode;
        },
        function (newMode) {
          if (newMode === $scope.modeType.equalResult) {
            $scope.interval = {
              value : 24,
              options: {
                floor: 1,
                ceil: 1600,
                translate: function(num) {
                  return num + 'h';
                }
              }
            }
          } else if (newMode === $scope.modeType.equalResponse) {
            $scope.interval = {
              value : 2,
              options: {
                floor: 2,
                ceil: 10,
                translate: function(num) {
                  return num + 'sec';
                }
              }
            }
          } else if (newMode === $scope.modeType.minBackup) {
            $scope.interval = {
              options : {
               disable: true
              }
            }
          }
        }
    );
  })
  .directive('searchBar', function (cloudberryConfig) {
    if(cloudberryConfig.removeSearchBar) {
      return {
        restrict: "E",
        controller: "SearchCtrl",
        template: [
          '<div class="btn-group search-keyword-btn-group col-lg-12">',
            '<button type="button" data-toggle="dropdown" class="btn btn-primary search-keyword-btn dropdown-toggle">Keywords List <span class="caret"></span></button>',
            '<ul class="dropdown-menu" aria-labelledby="dropdownMenu1">',
              '<li ng-repeat="keyword in predefinedKeywords"><a href="#" ng-click="predefinedSearch(keyword)">{{ keyword }}</a></li>',
            '</ul>',
          '</div>'
        ].join('')
      };
    } else {
      return {
        restrict: "E",
        controller: "SearchCtrl",
        template: [
          '<form class="form-inline" id="input-form" ng-submit="search()" >',
            '<label>',
            '<input type="radio" ng-model="mode" ng-value="modeType.equalResult">',
              '  Equal Result Size  ',
            '</label>',
            '<label>',
            '<input type="radio" ng-model="mode" ng-value="modeType.equalResponse">',
              '  Equal Response Time  ',
            '</label>',
            '<label>',
            '<input type="radio" ng-model="mode" ng-value="modeType.minBackup">',
              '  One Main and Many Backups',
            '</label><br/>',
            '<div>',
            '<rzslider rz-slider-model="interval.value" rz-slider-options="interval.options"></rzslider>',
            '</div>',
            '<div class="input-group col-lg-12">',
              '<label class="sr-only">Keywords</label>',
              '<input type="text" style="width: 97%" class="form-control " id="keyword-textbox" placeholder="Search keywords, e.g. zika" ng-model="keyword" required/>',
              '<span class="input-group-btn">',
                '<button type="submit" class="btn btn-primary" id="submit-button">Submit</button>',
              '</span>',
            '</div>',
          '</form>'
        ].join('')
      };
    }
  })
  .controller('ExceptionCtrl', function($scope, $window, cloudberry) {
    $scope.$watch(
      function() {
        return cloudberry.errorMessage;
      },

      function(newMsg) {
        if (newMsg) $window.alert(newMsg);
        cloudberry.errorMessage = null;
      }
    );
  })
  .directive('exceptionBar', function () {
    return {
      restrict: "E",
      controller: 'ExceptionCtrl',
      template: [
        '<p> {{ newMsg }}</p>'
      ].join('')
    }
  })
  .controller('D3Ctrl', function($scope, $http, $timeout, cloudberry) {

  });
