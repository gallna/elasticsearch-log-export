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
        s3.getObject(object.Key, this.processObject);
    };

    this.processObject = function(data) {
        //console.log("Metadata", data.Metadata);
        processor.process(data.Body, context);
    };
};
