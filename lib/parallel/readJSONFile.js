rjs.define("jsMR/lib/parallel/readJSONFile", [
    "fs", 
    "lazy",
    "Function/splitCb"
    ], function(fs, lazy){

    // readJSONFile({
    //     file : file,
    //     start : start,
    //     end : end,
    //     onLine : onLine(object, lineNumber, cb) 
    //     onEnd : onEnd
    // });
      
      return function(options){
        var file = options.file
          , start = options.start
          , end = options.end
          , onLine = options.onLine
          , onEnd = options.onEnd
          , st = fs.createReadStream(file, {
            start : start,
            end : end
          });

        var i = 0, toCompute = {};
        new lazy(st).lines.forEach(function(line){
            var id = i;
            toCompute[id] = true;
            onLine(JSON.parse(line), id, function(j){
                //console.log("onLine cb")
                typeof(toCompute[j]) === "function" && toCompute[j]();
                delete toCompute[j];
            }.curry(id));
            i++;
        });

        st.on("end", function(){
            var keys = Object.keys(toCompute);
            var cbs = onEnd.splitCb(keys.length);
            //console.log("stream end", options, keys.length, toCompute);
            if(keys.length > 1){
              for(var i = 0; i < keys.length; i++){
                toCompute[keys[i]] = cbs[i];
              }
            } else if(keys.length == 1){
              toCompute[keys[0]] = onEnd;
            } else {
              onEnd();
            }
        });

    }
});