var rjs = require("requirejs");
rjs.define("jsMR/tests/mapReduce", [
  "jsMR/lib/parallel/mapReduce", 
  "vows", 
  "assert", 
  "fs", 
  "cluster"], function(mR, vows, assert, fs, cluster){

    var output = "./output.json", input = "./out.json", output2 = "./output2.json";

    var topic = function(){
      var topic = this;

      mR({
          input : input,
          output : output,
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

          reduce : function(keySKU, countObjVals) {
            reducedVal = { count: 0, qty: 0 };

            for (var idx = 0; idx < countObjVals.length; idx++) {
              reducedVal.count += countObjVals[idx].count;
              reducedVal.qty += countObjVals[idx].qty;
            }

            return reducedVal;
          },

          finalize : function (key, reducedVal) {
            reducedVal.avg = reducedVal.qty/reducedVal.count;
            return reducedVal;
          },

          callback : function(err){
            topic.callback(err);
          }
      });
    };

    if(cluster.isWorker){
      return {
        run : topic
      };
    }

    return vows.describe("Basic map reduce").addBatch({
        'mongodb example' : {
          topic : topic,

          'is working' : function(err){
            var res = JSON.parse(fs.readFileSync(output, "utf8").split("\n")[0]);
            assert.isNumber(res.value.count);
            assert.isNumber(res.value.qty);
            assert.isNumber(res.value.avg);
          }

        },

        'map only example' : {
          topic : function(){
            mR({
                input : input,
                output : output2,
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

                reduce : function(keySKU, countObjVals) {
                  reducedVal = { count: 0, qty: 0 };

                  for (var idx = 0; idx < countObjVals.length; idx++) {
                    reducedVal.count += countObjVals[idx].count;
                    reducedVal.qty += countObjVals[idx].qty;
                  }

                  return reducedVal;
                },

                finalize : function (key, reducedVal) {
                  reducedVal.avg = reducedVal.qty/reducedVal.count;
                  return reducedVal;
                },

                callback : function(err){
                  topic.callback(err);
                }
            });
          },

          'is working' : function(){

          }
        }
    });
});