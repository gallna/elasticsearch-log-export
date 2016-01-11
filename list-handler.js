var http = require('http');

module.exports = function(s3, processor, host, uri) {

    this.handle = function(prefix) {
        console.log("Prefix: '" + prefix);
        s3.listObjects(prefix, this.processList);
    },

    this.processList = function(err, data) {
        if (err) {
            console.log("Errorr", err, err.stack);
        } else {
            console.log("Recieved '" + data.Name + "." + data.Prefix + "' list ");
            var unique = [];
            data.Contents.forEach(function(object) {
                var folder = object.Key.split("/").slice(0, -1).join("/");

                if (unique.indexOf(folder) == -1) {
                    unique.push(folder);
                }

            });
            unique.forEach(function(object) {
                uri.pathname ="/"+processor+"/"+ s3.bucket+"/"+object;
                //console.log(host, uri.format(uri));
                var options = {};
                options.host = host.split(":")[0];
                options.port = (host.split(":").length == 2) ? host.split(":")[1] : 80;
                options.path = uri.format(uri);
                sendRequest(options);
            });
        }
    };
};

function sendRequest (options) {
    //console.log(options); return;
        var request = http.request(options, function(response) {
            var body = '';
            response.on('data', function (chunk) {
                body += chunk;
            });
            response.on('end', function () {
                console.log(body);
            });
        }).on('error', function(e) {
            console.log(e);
        });
        request.end();
    }
