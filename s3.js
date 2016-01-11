var AWS = require('aws-sdk');

module.exports = function (bucket, accessKey, secretKey) {
    var s3 = {
        s3: null,
        bucket: bucket,

        listObjects: function(prefix, callback) {
            this.s3.listObjects({
                Bucket: this.bucket,
                Prefix: prefix
            }, callback);
        },

        getObject: function(key, callback) {
            this.s3.getObject({
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
            this.s3.getObject({
                Bucket: this.bucket,
                Key: key
            }).createReadStream().pipe(stream);;
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
    s3.s3 = new AWS.S3();
    return s3;
};
