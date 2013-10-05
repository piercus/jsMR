var rjs = require("requirejs");
rjs.define("paralleljs/parallel", [
  "paralleljs/fibo",
  "child_process", 
  "fs", 
  "cluster"], function(fibo, c, fs, cluster){
  var f = function(){
    var err = fs.openSync('./out.log', 'a');

    // var seq = c.spawn('seq',['-w',1,2], {stdio : "pipe"});
    // var prll = c.spawn('parallel',
    //   [
    //   '/home/pier/.nvm/v0.8.16/bin/node', 
    //   'index.js',
    //   '-a',
    //   'test.txt'
    //   ],{
    //     detached : true,
    //     stdio : "pipe"
    // });

    // seq.on("data", function(data){
    //   prll.stdin.write(data);
    // });

    // seq.on('close', function (code) {
    //   //if (code !== 0) {
    //   console.log('seq process exited with code ' + code);
    //   //}
    //   prll.stdin.end();
    // });
    var date = new Date();
    if(cluster.isMaster){
      console.log("here master");
      cluster.fork();
      cluster.fork();
      fibo(i);
      var date = new Date();
      var i = 42;
      var f = fibo(i);
      console.log("master : "+new Date() - date+ 'ms'+ '  fibo('+i+') => '+f);
      process.exit();
    } else {
      console.log("here worker");      
      var date = new Date();
      var i = 42;
      var f = fibo(i);
      console.log("worker : "+new Date() - date+ 'ms'+ '  fibo('+i+') => '+f);
      process.exit();
    }
  };

  return f;

});