'use strict';

var Consumer = require('../../common/kafka/consumer.js');
var Producer = require('../../common/kafka/producer.js');

module.exports = function(server) {
    var datasource = server.datasources.kafkads.settings;
    var consumer = new Consumer();
    consumer.consume(datasource);

    var producer = new Producer();
    producer.getConnection();
};
