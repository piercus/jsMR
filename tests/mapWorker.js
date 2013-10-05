var rjs = require("requirejs");
rjs.define("jsMR/tests/mapWorker", [
  "jsMR/lib/parallel/MapWorker", 
  "vows", 
  "assert", 
  "jsMR/lib/fs/getSplitPositions",
  "fs", 
  "cluster"], function(MapWorker, vows, assert, split, fs, cluster){

    var output = "./outWorker.json", 
        outputQuantile = "./outQuantile.json",
        input = "./out.json", 
        output2 = "./output2.json";

    var topic = function(){
      //console.log("bf split");
      split({ 
          input : input, 
          linePerSplit : 2000, 
          cb : function(err, res){
            new MapWorker({
                inputFile : input,
                start : res[4],
                end : res[5]-1, 
                map : function(emit, scope) {
                  for (var idx = 0; idx < this.items.length; idx++) {
                    var key = this.items[idx].sku;
                    var value = {
                      count: 1,
                      qty: this.items[idx].qty
                    };
                    emit(key, value);
                  }
                },
                reduce : function(keySKU, countObjVals){
                  reducedVal = { count: 0, qty: 0 };

                  for (var idx = 0; idx < countObjVals.length; idx++) {
                    reducedVal.count += countObjVals[idx].count;
                    reducedVal.qty += countObjVals[idx].qty;
                  }

                  return reducedVal;
                },
                callback : topic.callback,
                outQuantiles : outputQuantile,
                outuptFile : output,
                nQuantiles : 1000
            });
          }
      });
      var topic = this;
    };

    return vows.describe("MapWorker Contructor").addBatch({
        'simple worker example' : {
          topic : topic,

          'is working' : function(err){
            var res = JSON.parse(fs.readFileSync(output, "utf8").split("\n")[0]);
            assert.isNumber(res.value.count);
            assert.isNumber(res.value.qty);
          },

          'has quantiles working' : function(err){
            var res = JSON.parse(fs.readFileSync(outputQuantile, "utf8"));
            assert.isNumber(res.n);
            assert.isNumber(res.nQuantiles);
            assert.isNumber(res.quantiles[0].line);
            assert.isString(res.quantiles[0].key);
            assert.equal(res.quantiles.length, res.nQuantiles+1);
          }

        }
    });
    

});