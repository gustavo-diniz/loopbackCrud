'use strict';

var app = require('../../server/server');
var producer   = require("../kafka/producer.js");

module.exports = function(Wskafka) {
    
    Wskafka.postProducer = function(msg,cb) {

        var message = {message: 'Post recebido !', retorno:msg.mensagem};
        var datasource = app.datasources.Kafkads.settings;
        
        var prod = new producer();
        var connection = prod.getConnection();
        prod.enviar(connection, datasource, msg, function (err, data) {
            
            if(err != null){
                console.log("erro ao enviar mensagem: "+err);
                var message = {message: 'Erro ao enviar mensagem'};
                
                cb(null, message);   
            
            }else{
                var message = {message: 'Executado com sucesso !'};
                cb(null, message);  
            }
        }); 
    }

    Wskafka.remoteMethod(
        'postProducer', {
          http: {
            path: '/postProducer',
            verb: 'post'
          },
          accepts: {
            arg: 'msg', 
            type: 'object'
          },
          returns: {
            arg: 'postProducer',
            type: 'string'
          }
        }
    );
};
