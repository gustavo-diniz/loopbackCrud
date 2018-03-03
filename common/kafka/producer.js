var kafka = require('kafka-node');

global._producer = null;

var KafkaProducer = function (){
}

KafkaProducer.getConnection = function(){

    if(global._producer == null){
        var Producer = kafka.Producer,
            client = new kafka.Client(),
            producer = new Producer(client);

            global._producer = producer;

            producer.on('ready', function () {
                console.log('Producer is ready');
            });
        
            producer.on('error', function (err) {
                console.log('Producer is in error state');
                console.log(err);
            })
    }

    return global._producer;

    
}

KafkaProducer.disconnect = function(producer){
    producer.close(function(){
        console.log("desconectado !");
    });
}

KafkaProducer.enviar = function(producer, file, msg, callback){
    var payloads = [
                    { topic: file.topic, 
                         messages:JSON.stringify(msg), 
                         partition: file.partition 
                        }
                    ];

        producer.send(payloads, callback);
       
}

module.exports = function(){
    return KafkaProducer;
};