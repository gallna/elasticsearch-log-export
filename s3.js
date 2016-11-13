var AWS = require('aws-sdk');

module.exports = function (bucket) {
    var s3 = {
        bucket: bucket,

        listObjects: function(prefix, callback) {
            var s3 = new AWS.S3();
            s3.listObjects({
                Bucket: this.bucket,
                Prefix: prefix
            }, callback);
        },

        getObject: function(key, callback) {
            var s3 = new AWS.S3();
            s3.getObject({
                Bucket: this.bucket,
                Key: key
            }, function(err, data) {
                if (err) {
                    console.log("processObject error", err, err.stack);
                } else {
                    //console.log("Metadata", data.Metadata);
                    callback(data);
                }
            });
        },

        pipeObject: function(key, stream) {
            var s3 = new AWS.S3();
            s3.getObject({
                Bucket: this.bucket,
                Key: key
            }).createReadStream().pipe(stream);;
        },

        setMetadata: function(object, metadata) {
            var s3 = new AWS.S3();
            s3.putObject({
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

    return s3;
};
