

module.exports = function(s3, processor) {
    this.handle = function(prefix) {
        console.log("Prefix: '" + prefix);
        s3.listObjects(prefix, this.processList);
    },

    this.processList = function(err, data) {
        if (err) {
            console.log("Errorr", err, err.stack);
        } else {
            console.log("Recieved '" + data.Name + "." + data.Prefix + "' list ");
            //data.Contents.forEach(this.processObject);
            data.Contents.forEach(function(object) {
                var ObjectHandler = require('./handler/base-handler.js');
                var objHandler = new ObjectHandler(object, s3, processor);
                objHandler.handle();
                objHandler = null;
            });
        }
    };

    this.processObject = function(object) {
        var objHandler = this.objectHandler;
        var objectHandler = new objHandler(this.s3, this.processor);
        objectHandler.processObject(object);
    };
};
