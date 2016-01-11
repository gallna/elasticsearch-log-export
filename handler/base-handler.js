var zlib = require('zlib');

module.exports = function (object, s3, processor) {

    var context = {
        fail: function (error) {
            console.log("The error:" + error);
        },
        succeed: function (message) {
            console.log("The success: " + object.Key + ": " + message);
            // this.setMetadata()
        }
    };

    this.handle = function() {
        console.log("Processing " + object.Key);
        var gunzip = zlib.createGunzip();
        s3.pipeObject(object.Key, gunzip);
        // this.processObject
        gunzip.on('data', function(data) {
            // decompression chunk ready, add it to the buffer
            //console.log(data.toString('utf8'));
            var chunk = data.toString('utf8');
            processor.process(chunk, context);
            //buffer.push(data.toString())

        //}).on("end", function() {
            // response and decompression complete, join the buffer and return
            //callback(null, buffer.join(""));

        }).on("error", function(e) {
            console.log("base-handler error: ", e);
        });

    };

    this.processObject = function(data) {
        //console.log("Metadata", data.Metadata);
        processor.process(data.Body, context);
    };
};
