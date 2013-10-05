var rjs = require("requirejs");
rjs.define("jsMR/lib/parallel/readFile", [
    "os", 
    "fs", 
    "cluster", 
    "require", 
    "path",
    "lazy",
    "utils/objMapAsync",
    "jsMR/lib/parallel/writeJSONFile",
    "jsMR/lib/parallel/readJSONFile",
    "Function/curry", 
    "Function/splitCb"
    ], function(os, fs, cluster, require, path, lazy, objMapAsync, writeJSONFile, readJSONFile){
    
    var getOutFile = function(file, index){
      return "/home/pier/.tmp/"+index+path.basename(file);
    };

    return function(options){
      var file = options.file
        , onLine = options.onLine
        , onEnd = options.onEnd
        , n = 100
        , callback = options.callback
        , maxJobs = 2;


      if(cluster.isMaster){
        require(["jsMR/lib/fs/getSplitPositions"], function(split){

            var n_ = n || os.cpus().length;

            split(file, n_, function(err, res){
                var jobs = [], files = [];
                for(var i = 0; i < n_; i++){
                  var f = getOutFile(file,i);
                  files.push(f);
                  jobs.push({
                      start : res[i],
                      end : res[i+1]-1,
                      index : i,
                      file : f,
                      percent : Math.floor(i/n*100)
                  });
                }
                console.log(jobs);
                var callbacked = false;
                var nextJob = function(){
                  if(jobs.length > 0 && Object.keys(cluster.workers).length < maxJobs) {
                    var j = jobs.shift();
                    process.stdout.write("percent : "+j.percent+"%\r");
                    cluster.fork(j);
                  } else if(jobs.length == 0 && Object.keys(cluster.workers).length == 0 && !callbacked) {

                    callback(null, files);
                    callbacked = true;
                  }
                };

                cluster.on('exit', nextJob);

                for(var i = 0; i < maxJobs; i++){ nextJob(); }
            });

        });
      } else if(cluster.isWorker){
        var fileOut = process.env.file
          , out = fs.createWriteStream(fileOut)
          , start = parseInt(process.env.start)
          , end = parseInt(process.env.end)
          , index = parseInt(process.env.index)
          , st = fs.createReadStream(file, {
                start : start,
                end : end
            });
        var i = 0, toCompute = {};
        //console.log("start map stream", file);
        readJSONFile({
            file : file,
            start : start,
            end : end,
            onLine : onLine,
            onEnd : onEnd.curry(function(err, res){
              //console.log("write",res);
              writeJSONFile({
                  obj : res,
                  file : fileOut,
                  onEnd : function(){
                    //console.log("file wrote, exit worker");
                    process.exit();
                  }
              });
            }) 
        });

      }

    };

});