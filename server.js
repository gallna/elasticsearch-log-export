var http = require('http');
var url = require('url');
var S3 = require('./s3.js');
var Processor = require('./processor/accessLog.js');
var Handler = require('./handler.js');
var ListHandler = require('./list-handler.js');
var ObjectHandler = require('./handler/base-handler.js');
var Elasticsearch = require('./elasticsearch.js');

//Lets define a port we want to listen to
const PORT=8089;

var elasticsearchConfig = function (params) {
    var config = {
        endpoint: "elasticsearch",
        port: 9200,
        indexName: "index-name",
        typeName: "type-name",
        chunkSize: 2000
    };
    for (var param in params) {
        config[param] = params[param];
    }
    return config;
};

function exportList (bucket, prefix, host, uri) {
    var handler = new ListHandler(
        new S3(bucket, process.env.ACCESS_KEY_ID, process.env.SECRET_ACCESS_KEY),
        "export",
        host,
        uri
    );
    handler.handle(prefix);
}

function exportFromS3 (bucket, prefix, esConfig) {
    var handler = new Handler(
        new S3(bucket, process.env.ACCESS_KEY_ID, process.env.SECRET_ACCESS_KEY),
        new Processor(esConfig, new Elasticsearch(esConfig)),
        ObjectHandler
    );
    handler.handle(prefix);
}

//We need a function which handles requests and send response
function handleRequest(request, response){
    var uri = url.parse(request.url, true);
    var pathname = uri.pathname.split("/");

    switch(pathname[1]) {
        case 'list':
            var bucket = pathname[2];
            var prefix = pathname.slice(3).join("/")
            response.end('Exporting: '+ bucket +':' + pathname.slice(2).join("/"));
            exportList(bucket, prefix, request.headers.host, uri);

            break;
        case 'export':
            var bucket = pathname[2];
            var prefix = pathname.slice(3).join("/")
            var esConfig = new elasticsearchConfig(uri.query);
            exportFromS3(bucket, prefix, esConfig);
            response.end('Exporting: '+ bucket +':' + pathname.slice(2).join("/"));
            break;
        default:
            response.end('Not found: ' + request.url);
    }
}

//Create a server
var server = http.createServer(handleRequest);

//Lets start our server
server.listen(PORT, function(){
    console.log("Server listening on port %s", PORT);
});
