var rjs = require("requirejs");

rjs.config({
    baseUrl: "/home/pier/Programmation",
    nodeRequire: require
});

if (typeof(sand) !== "undefined"){
  console.log("[ERROR] sand is already defined");
}

(function(){
  Array.prototype.last = String.prototype.last = function() {
    return this[this.length - 1];
  };
 
  var parse = function(name){return name.split('/').last()};
  
  GLOBAL.sand = {
    define : function(name, requires, fn, options){
      if (typeof(requires) === 'function') {
        fn = requires;
  requires = [];
      } else if (typeof(requires) === 'undefined') requires = []; 
      var amdFn = function(){
  var r = {};
  for (var i = 0; i < arguments.length; i++){
    var alias = requires[i].split("->")[1] || parse(requires[i]);
    r[alias] = arguments[i];
        }                         
        return fn(r);
      }
      
      rjs.define(name, requires, amdFn);
    }
  };
 
})();

rjs(["jsMR/lib/parallel/readFile"], function(readFile){
    readFile("./out2.json", 2, function(){
      console.log("end");
      process.exit();
    })
});