rjs.define("jsMR/lib/parallel/MapWorker", [
  "Seed/Seed", 
  "fs",
  "jsMR/lib/parallel/readJSONFile",
  "jsMR/lib/parallel/writeJSONFile",
  "progress"], 
  function(Seed, fs, readJSONFile, writeJSONFile, ProgressBar){
    return Seed.extend({
        "options" : {
          inputFile : null,
          start : null,
          end : null, 
          map : null,
          reduce : null,
          callback : null,
          outQuantiles : null,
          outuptFile : null,
          nQuantiles : 10
        },

        "+init" : function(){
          
          this.reduceO = {};
          //console.log("red JSON File");
          readJSONFile({
              file : this.inputFile,
              start : this.start,
              //progressBar : new ProgressBar("Worker ", { total : })
              end : this.end,
              onLine : this.onLine.bind(this),
              onEnd : this.onEndReading.bind(this)
          });
        },

        onEndReading : function(){
          var res = {}, n = 0;

          for(var i in this.reduceO){
            n++;
            if(this.reduceO[i].length == 1){
              res[i] = this.reduceO[i][0];
            } else {
              res[i] = this.reduce(i, this.reduceO[i]);
            }
          }
          //console.log("bf sorting", nQuantiles);

          var keys = Object.keys(res),
              sortedKeys = keys.sort(function(a, b){
                if(typeof(a) === "number"){
                  if(typeof(b) === "number"){
                    return (a-b);
                  }
                  return -1;
                } else if(typeof(b) === "number"){
                  return 1;
                } else if(typeof(a) === "string"){
                  if(typeof(b) === "string"){
                    return a.localeCompare(b);
                  }
                  return -1;
                } else if(typeof(b) === "string"){
                  return 1;
                }
              }),
              n = keys.length, 
              quantiles = [{ key : sortedKeys[0], line : 1}],
              nQuantiles = this.nQuantiles;
          //console.log("af sorting", nQuantiles);

          for(var i = 0; i < n; i ++){
            while((i+1)*nQuantiles/n >= quantiles.length){
              quantiles.push({line : i+1, key : sortedKeys[i]});
            }
          }
          console.log("number of reduce keys : ", sortedKeys.length);
          //return;
          writeJSONFile({
              obj : res,
              sortedKeys : sortedKeys,
              file : this.outuptFile,
              progressBar : new ProgressBar('writing :percent elapsed :elapsed s :bar', {total : sortedKeys.length, width: 40}),
              onEnd : function(){
                //new line
                console.log("");

                fs.writeFile( this.outQuantiles, JSON.stringify({
                    quantiles : quantiles,
                    nQuantiles : nQuantiles,
                    n : n
                  }), function (err) {
                    if (err) throw err;
                    this.callback();
                    process.exit();
                }.bind(this));

              }.bind(this)
          });

          this.reduceO = null;
        },

        onLine : function(o, id ,cb){
                
          var emit = function(k,v){
            this.reduceO[k] || (this.reduceO[k] = []);
            this.reduceO[k].push(v);
          }.bind(this);

          this.map.call(o, emit);
          cb();
        }
    });
});