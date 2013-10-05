rjs.define("jsMR/lib/parallel/writeJSONFile", [
    "fs", 
    "utils/objMapAsync"
    ], function(fs, objMapAsync){

    // writeJSONFile({
    //     file : file,
    //     obj : JS Object,
    //     onEnd : onEnd
    // });
      
      return function(options){
        //console.log(options.file);
        var file = options.file
          , onEnd = options.onEnd
          , obj = options.obj
          , progressBar = options.progressBar
          , sortedKeys = options.sortedKeys
          , st = fs.createWriteStream(file);

        objMapAsync(obj, function(v, k, cb1){
          st.write(JSON.stringify({_id : k, value : v})+"\n", cb1);
        }, { 
          sortedKeys : sortedKeys,
          progressBar : progressBar 
        }, onEnd);

    }
});