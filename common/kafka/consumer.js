var kafka         = require('kafka-node');

var KafkaConsumer = function(){
}

KafkaConsumer.consume = function(file){
      console.log(
        'topic = '            + file.topic +
        '\nautoCommit = '     + file.autocommit + 
        '\nfetchMaxWaitMs = ' + file.fetchmaxwaitms + 
        '\nhost = '           + file.host
      );

      var HighLevelConsumer = kafka.HighLevelConsumer;
      var Client            = kafka.Client;
      var topic             = file.topic;
    
      var client            = new Client(file.host);
      var topics            = [{ topic: topic }];
      var options           = { autoCommit: file.autocommit, 
                                fetchMaxWaitMs: file.fetchmaxwaitms, 
                                fetchMaxBytes: 1024 * 1024 
                              };
    
      var consumer          = new HighLevelConsumer(client, topics, options);

      consumer.on('message', function (message) {
        console.log("Mensagem do consumidor");
        console.log(message);
      });

      consumer.on('error', function (err) {
        console.log('error', err);
      });

}


module.exports = function(){
    return KafkaConsumer;
}