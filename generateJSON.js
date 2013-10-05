var rjs = require("requirejs");

rjs.config({
    baseUrl: "/home/pier/Programmation",
    nodeRequire: require
});

rjs(["jsMR/tests/generate"], function(g){
    g(1000000, function(){
        console.log("done");
        process.exit();
    })
});