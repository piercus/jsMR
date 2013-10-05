var rjs = require("requirejs");
rjs.define("jsMR/lib/fs/getSplitPositions", [
  "fs", 
  "progress",
  "child_process",
  "Function/splitCb", 
  "Array/mapAsync"], function(fs, Bar, child_process){


    var exec = child_process.exec;



    return function(options){

      exec('wc -l '+options.input, function (error, results) {

          // we need a line break at each 16MB = 16777216 B
          var bufferTmp = new Buffer(16777216)
            , file = options.input
            , size = fs.statSync(file).size
            , nLines = parseInt(results.split(" "))
            , res = [0]
            , arr = []
            , cb = options.cb
            , n = Math.ceil(nLines/options.linePerSplit)
            , partSize = Math.floor(size/n);
          console.log(n);

          var findLineBreak = function(o, i, cb_){
              var st = fs.createReadStream(file,{
                  start : partSize*i,
                  encoding : "utf8",
                  bufferSize : 1
              });
              
              var cursor = partSize*i;
              //console.log(i);
              var onData = function(data){
                  if(data === "\n"){
                    res.push(cursor);
                    st.pause();
                    fs.closeSync(st.fd);
                    cb_(null, cursor+1);
                    return
                  }
                  cursor++;
                  if(cursor === (partSize)*(i+1)){
                    cb_("cannot cut the file" + path);
                  }
              };
              st.on("data", onData);

              st.on("error", function(i, err){
                console.log(" error ", i, err);
                throw(err)
              }.curry(i));
          };
          //console.log("bf push");

          for(var i = 1; i < n; i ++){
            arr.push(i);
          }

          //console.log("bf ma Sync", arr.length);

          arr.mapAsync(findLineBreak, 
            //Without stepByStep, error: EMFILE, open for i == 1018
            // linux only accept 1024 file descriptors
            {nParallel : 500, progressBar : new Bar('spliting :percent elapsed :elapsed s :bar',{width : 40, total: arr.length})}, 
            function(err, res){
              console.log("");
              res.unshift(0);
              res.push(size);
              var resS = res.sort(function(a,b){
                return a - b;
              });
              cb(null, resS);
          });
      });


      
    };

});