var zlib = require('zlib');
var dns = require("dns");

function apacheLogLine (results) {
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

function esLog (esConfig, line, log) {
    var logLine = apacheLogLine(log);
    var logTime = logLine.dateTime.match(/([^:]+):([^ ]+) (.*)/);
    logTime[1] = logTime[1].replace(/\//g, ' ');
    var timestamp = new Date(logTime[1] + ' ' + logTime[2]);
    logLine['@timestamp'] = timestamp.toISOString();

    var action = { "index": {} };
    action.index._index = esConfig.indexName;
    action.index._type = esConfig.typeName;

    return [
        JSON.stringify(action),
        JSON.stringify(logLine),
    ].join('\n');
};

function apacheLogs (logLines, esConfig) {
    regexp = /([^ ]+)\s([^ ]+)\s\((.*)\)\s([^ ]+)\s([^ ]+)\s\[(.*)\]\s"([^"]+)"\s(\d+)\s([\d-]+)\s"([^"]+)"\s"([^"]+)"/;
    var bulkRequestBody = '';
    logLines.forEach(function(line) {
        var log = line.match(regexp);
        if (log) {
            bulkRequestBody += esLog(esConfig, line, log) + '\n';
        } else {
            //console.log("not recognized: " + line);
        }
    });
    return bulkRequestBody;
}

function processor (inputBuffer, elasticsearch, esConfig, context) {
    //var inputBuffer = new Buffer(inputBuffer, 'base64');
    // decompress the input
    zlib.gunzip(inputBuffer, function(error, buffer) {
        if (error) {
            console.log(error);
            return;
        }
        var data = buffer.toString('utf8');
        var logLines = data.split("\n");
        var logs = null;
        while(logLines.length) {
            logs = apacheLogs(logLines.splice(0, esConfig.chunkSize), esConfig);
            // skip control messages
            if (!logs) {
                console.log('Received a control message');
                context.succeed();
                return;
            }
            elasticsearch.send(logs, context);
            logs = null;
        }
        return;
    });
}

function process (data, elasticsearch, esConfig) {
    var logLines = data.split("\n");
    var logs = null;
    while(logLines.length) {
        logs = apacheLogs(logLines.splice(0, esConfig.chunkSize), esConfig);
        // skip control messages
        if (!logs) {
            console.log('Received a control message');
            return;
        }
        elasticsearch.send(logs);
        logs = null;
    }
    return;
}

module.exports = function (esConfig, elasticsearch) {

    return {
        "process": function(inputBuffer, context) {
            process(inputBuffer, elasticsearch, esConfig, context);
        }
    };
};
