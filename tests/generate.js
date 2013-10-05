var rjs = require("requirejs");
rjs.define("jsMR/tests/generate", [
  "fs"], function(fs){

    return function(n, cb){
        var generateObj = function(i){
          var id = i,
              items = [];
          var nItems = Math.floor(Math.random()*100);
          for(var i = 0; i < nItems; i++){
            var sku = Math.floor(Math.random()*10000000);
            items.push({
                sku : sku,
                qty : Math.floor(Math.random()*10),
                price : Math.floor(Math.random()*500)/10
            });
          }

          return {
            _id: i,
            cust_id: "abc"+i,
            status: 'A',
            price: Math.floor(Math.random()*1000),
            items: items
          };
        };

        var out = fs.createWriteStream("./out.json");
        var next = function(j){
          if(j > n){
            return cb();
          }
          out.write(JSON.stringify(generateObj(j))+"\n", "utf8", function(){
            next(j+1);
          });
        };
        next(0);
    };

});