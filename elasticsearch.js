var http = require('http');

function sendRequest (requestParams, body, context) {
    var request = http.request(requestParams, function(response) {
        var listener = new responseListener(response);
        response.on('data', listener.onData);
        response.on('end', listener.onEnd.bind( {"context": context} ));
    }).on('error', function(e) {
        console.log('Error: ' + JSON.stringify(e, null, 2));
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
                console.log('Success: ' + JSON.stringify(success));
                return this.context.succeed('Success');
            }

            if (response.statusCode !== 200 || info.errors === true) {
                var error = {
                    "statusCode": response.statusCode,
                    "responseBody": responseBody
                };
                console.log('Error: ' + JSON.stringify(error, null, 2));
                if (failedItems && failedItems.length > 0) {
                    console.log("Failed Items: " +
                        JSON.stringify(failedItems, null, 2));
                }
            }
        }
    };
    return listener;
}

function buildRequest(endpoint, body) {
    var request = {
        host: endpoint,
        port: 9200,
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
            var requestParams = buildRequest(this.config.endpoint, body);
            sendRequest(requestParams, body, context);
        }
    };
};
