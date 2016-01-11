var AWS = require('aws-sdk');

var s3ObjectHandler = {
    s3: null,
    bucket: null,
    extractor:null,
    processObject: function(object, processor) {
        console.log("Processing " + object.Key);
        var context = this;
        this.s3.getObject({
            Bucket: this.bucket,
            Key: object.Key
        }, function(err, data) {
            if (err) {
                console.log("processObject error", err, err.stack);
            } else {
                //console.log("Metadata", data.Metadata);
                processor.process(data.Body, context);
            }
        });
    },

    fail: function (error) {
        console.log("The error:" + error);
    },
    succeed: function (message) {
        console.log("The success: " + message);
        // this.setMetadata()
    },

    setMetadata: function(object, metadata) {
        this.s3.putObject({
            Bucket: this.bucket,
            Key: object.Key,
            Metadata: metadata
        }, function(err, data) {
            if (err) {
                console.log("setMetadata error", err, err.stack);
            }
            //else     console.log(data);           // successful response
        });
    }
};

module.exports = function(bucket) {
    var handler = s3ObjectHandler;
    handler.s3 = new AWS.S3();
    handler.bucket = bucket;
    return handler;
};

