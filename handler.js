var ObjectHandler = require('./handler/base-handler.js');

var S3 = require('./s3.js');
var Processor = require('./processor/accessLog.js');
var ObjectHandler = require('./handler/base-handler.js');
var Elasticsearch = require('./elasticsearch.js');

module.exports = function(bucket, esConfig) {
    var s3 = new S3(bucket);
    var es = null;
    var processor = null;
    var objHandler = null;
    this.handle = function(prefix) {
        console.log("Prefix: '" + prefix);
        s3.listObjects(prefix, this.processList);
        return;
    },

    this.processList = function(err, data) {
        if (err) {
            console.log("Errorr", err, err.stack);
        } else {
            console.log("Recieved '" + data.Name + "." + data.Prefix + "' list ");
            //data.Contents.forEach(this.processObject);
            var callback = function () {
                if (data.Contents.length) {
                    var object = data.Contents.shift();
                    console.log("Shifted: '" + object.Key);
                    s3 = new S3(bucket);
                    es = new Elasticsearch(esConfig);
                    processor = new Processor(esConfig, es);
                    objHandler = new ObjectHandler(object, s3, processor);
                    objHandler.handle(callback);
                }
            };
            callback();
        }
    };

    this.processObject = function(object) {
        var objHandler = this.objectHandler;
        var objectHandler = new objHandler(this.s3, this.processor);
        objectHandler.processObject(object);
    };
};

