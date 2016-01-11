var http = require('http');

function sendRequest (requestParams, body, context) {
    var request = http.request(requestParams, function(response) {
        var listener = new responseListener(response);
        response.on('data', listener.onData);
        response.on('end', listener.onEnd.bind( {"context": context} ));
    }).on('error', function(e) {
        console.log(e);
        console.log('Elasticsearch error: ' + JSON.stringify(e, null, 2));
    });
    request.end(requestParams.body);
}


function responseListener (response) {
    var responseBody = '';

    var listener = {

        onData: function(chunk) {
            responseBody += chunk;
        },

        onEnd: function() {
            var info = JSON.parse(responseBody);
            var failedItems;
            var success;

            if (response.statusCode >= 200 && response.statusCode < 299) {
                failedItems = info.items.filter(function(x) {
                    if (x.index) {
                        return x.index.status >= 300;
                    }
                });

                success = {
                    "attemptedItems": info.items.length,
                    "successfulItems": info.items.length - failedItems.length,
                    "failedItems": failedItems.length
                };
                responseBody = null;
                return this.context.succeed('Success: ' + JSON.stringify(success));
            }

            if (response.statusCode !== 200 || info.errors === true) {
                var error = {
                    "statusCode": response.statusCode,
                    "responseBody": responseBody
                };
                console.log('Failed: ' + JSON.stringify(error, null, 2));
                if (failedItems && failedItems.length > 0) {
                    console.log("Failed Items: " + JSON.stringify(failedItems, null, 2));
                }
            }
            responseBody = null;
        }
    };
    return listener;
}

function buildRequest(endpoint, port, body) {
    var request = {
        host: endpoint,
        port: port,
        method: 'POST',
        path: '/_bulk',
        body: body,
        headers: {
            'Content-Type': 'application/json',
            'Host': endpoint,
            'Content-Length': Buffer.byteLength(body)
        }
    };

    return request;
}

var esConfig = {
    endpoint: null,
    indexName: null,
    typeName: null
};

module.exports = function (esConfig) {
    return {
        "config": esConfig,
        "send": function (body, context) {
            var requestParams = buildRequest(this.config.endpoint, this.config.port, body);
            sendRequest(requestParams, body, context);
        }
    };
};
