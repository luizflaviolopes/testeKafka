const kafka = require('kafka-node');

var teste = [];

try {
  const client = new kafka.KafkaClient({kafkaHost:'10.33.132.25:9092',clientId:'primeiro'});
  let consumer = new kafka.Consumer(
    client,
    [{ topic: 'TRON', partition: 0 }],
    {
      groupId: 'abc',
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      fromOffset: false
    }
  );


  consumer.on('message', async function(message) {
    var j = JSON.parse(message.value)
    teste.push(j);
    if(message.highWaterOffset-1 == message.offset)
      console.log(teste);
    //console.log({cpf:j.CPF, nome:j.NomeForm, hora:j.HoraEnviada});
  })
  consumer.on('error', function(err) {
    console.log('error', err);
  });
}
catch(e) {
  console.log(e);
}

