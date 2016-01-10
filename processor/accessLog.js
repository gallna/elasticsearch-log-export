// v1.1.3
var http = require('http');
var zlib = require('zlib');
var crypto = require('crypto');
var dns = require("dns");

var endpoint = '52.90.74.243';
var indexName = "zyx";
var typeName = "rwd-requests";

var apacheLogLine = function(results) {
    var regexp = /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})+/g;
    var line = {
        date: results[1],
        host: results[2],
        ip: results[3].match(regexp),
        authUser: results[4],
        authPass: results[5],
        dateTime: results[6],
        request: results[7],
        status: results[8] * 1,
        size: results[9] * 1,
        referer: results[10].length ? results[10] : "unknown",
        userAgent: results[11].length ? results[11] : "unknown"
    };
    if (line.ip && line.ip[0].match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/g)) {
        line.userIp = line.ip[0];
        // dns.reverse(line.userIp, function(err, hostnames) {
        //     line.hostnames = hostnames;
        //     console.log(hostnames);
        // });
    }
    return line;

};

var esLog = function(line, logLine) {
    var logTime = logLine.dateTime.match(/([^:]+):([^ ]+) (.*)/);
    logTime[1] = logTime[1].replace(/\//g, ' ');
    var timestamp = new Date(logTime[1] + ' ' + logTime[2]);
    logLine['@timestamp'] = timestamp.toISOString();

    var action = { "index": {} };
    action.index._index = indexName;
    action.index._type = typeName;

    return [
        JSON.stringify(action),
        JSON.stringify(logLine),
    ].join('\n');
};

var apacheLogs = function(data) {
    regexp = /([^ ]+)\s([^ ]+)\s\((.*)\)\s([^ ]+)\s([^ ]+)\s\[(.*)\]\s"([^"]+)"\s(\d+)\s([\d-]+)\s"([^"]+)"\s"([^"]+)"/;
    var logLines = data.split("\n");
    var bulkRequestBody = '';
    logLines.forEach(function(line) {
        var log = line.match(regexp);
        if (log) {
            var logLine = esLog(line, new apacheLogLine(log));
            bulkRequestBody += logLine + '\n';
        } else {
            console.log("not recognized: ".log);
        }
    });
    return bulkRequestBody;
};

function processor (inputBuffer, elasticsearch, context) {

    //var inputBuffer = new Buffer(inputBuffer, 'base64');
    // decompress the input
    zlib.gunzip(inputBuffer, function(error, buffer) {
        if (error) {
            throw (error);
        }
        var data = buffer.toString('utf8');
        var logs = apacheLogs(data);

        // skip control messages
        if (!logs) {
            console.log('Received a control message');
            context.succeed('Control message handled successfully');
            return;
        }

        // post documents to the Amazon Elasticsearch Service
        elasticsearch.send(logs, context);
    });
};

module.exports = function (elasticsearch) {

    return {
        "process": function(inputBuffer, context) {
            processor(inputBuffer, elasticsearch, context);

        }
    };
};
