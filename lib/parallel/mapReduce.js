rjs.define("jsMR/lib/parallel/mapReduce", [
  "Seed/Seed", 
  "fs",
  "jsMR/lib/parallel/readFile",
  "jsMR/lib/parallel/readJSONFile",
  "jsMR/lib/parallel/writeJSONFile",
  "Array/mapAsync"], 
  function(Seed, fs, readFile, readJSONFile, writeJSONFile){

    var MapReduce = Seed.extend({
        "options" : {
          input : null,
          output : null,
          reduce : null,
          map : function(emit){ emit(this._id, this);},
          reduce : null,
          finalize : function(k,o){ return o},
          callback : null,
          n : null
        },

        "+init" : function(){
          var startingTime = new Date();
          var reduceO = {}, reducer = this.reduce;
          
          readFile({
              file : this.input, 
              onLine : function(o, id, cb){
                var localRes = {};
                
                var emit = function(k,v){
                  reduceO[k] || (reduceO[k] = []);
                  reduceO[k].push(v);
                };

                this.map.call(o, emit);
                cb();

              }.bind(this), 
              onEnd : function(cb){
                var res = {};

                for(var i in reduceO){
                  if(reduceO[i].length == 1){
                    res[i] = reduceO[i][0];
                  } else {
                    res[i] = reducer(i, reduceO[i]);
                  }
                }
                //console.log(res);
                cb(null, res);
                reduceO = null;
              }.bind(this),
              callback : function(err, files){
                if(reducer){
                  // if a reduce function is provided, we need to reduce files
                  this.reduceFiles(files, this.callback)  
                } else {
                  // if no reduce function we just concat out files
                  this.mergeFiles(files, this.callback);                 
                }

              }.bind(this)
          });
        },
        "mergeFiles" : function(files, cb){
           var spawn = child_process.spawn,
               out = fs.openSync(this.output, 'w');

           var child = spawn('cat', files, {
             detached: true,
             stdio: [ 'ignore', out, err ]
           });

           child.on("end", function(){
              cb(null);
           });
        },
        "reduceFiles" : function(files, cb){
          var out = fs.openSync(this.output, 'w');
          var reduceO = {}, reducer = this.reduce;
          //console.log("files", files)
          //
          // F the number of files from map-operation
          // l1 the average number of lines per file
          // L maximum number of objects in reduceO

          // if(F*l1 > L) 
          //
          // We need to split each file so each file has less than L/F lines
          // let lmax be L/F
          //
          // nSplit = floor(l/lmax)
          // we need to get nSplit-quantile of the _ids
          //
          // see something like http://www.cs.umd.edu/~samir/498/manku.pdf
          //
          //
          // Split each file into nSplit files distributing objects in nSplit-quantiles
          // 
          // reduce files in the same quantile
          //

          files.mapAsync(function(v,k,cb1){
              //console.log(v);
              readJSONFile({
                file : v,
                onLine : function(l, i, cb2){
                  var obj = l, id = obj._id;
                  reduceO[id] || (reduceO[id] = []);
                  reduceO[id].push(obj.value);
                  cb2(null);
                },
                onEnd : function(){
                  //console.log("end");
                  cb1()
                }
              });
              //console.log("readJSONFile")
            }, function(err){
              var res = {};

              for(var i in reduceO){
                if(reduceO[i].length == 1){
                  res[i] = reduceO[i][0];
                } else {
                  res[i] = this.finalize(i,reducer(i, reduceO[i]));
                }
              }
              //console.log("write reduce",res, reduceO);
              writeJSONFile({
                  obj : res,
                  file : this.output,
                  onEnd : cb
              });

              reduceO = null;
          }.bind(this));
        }
    });

    return function(o){
      return new MapReduce(o);
    };

});