var rjs = require("requirejs");
rjs.define("paralleljs/fibo", function(){
  var fibo = function(n){
    if(n == 1 || n == 2 || isNaN(n)){
      return 1;
    } else {
      return fibo(n-1)+fibo(n-2);
    }
  };

  return fibo;
});