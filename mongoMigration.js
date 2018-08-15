const mongo = require('mongodb');
const async = require('async');
const customer = require('./m3-customer-data.json');
const address = require('./m3-customer-address-data.json');
const url = 'mongodb://localhost:27017';
const dbName = 'edx-db';

const mongoClient = mongo.MongoClient;

var limit = parseInt(process.argv[2]) || 1000;
if(customer.length % limit !== 0){
    console.error("Error: must create divisible pipes");
}
var tasks = [];
var indexLimit = 0;
var pipeArray = [];

for(var i=0; i < customer.length/limit ; i++){
    var tempArray = [];
    for(var j=i*limit; j<indexLimit+limit ; j++){
        customer[j] = Object.assign(customer[j], address[j]);
        tempArray.push(customer[j]);
    }
    indexLimit += limit; 
    pipeArray.push(tempArray);
}

mongoClient.connect(url, {useNewUrlParser:true}, (err, client)=>{
    if(err){
        return process.exit(1);
    }
    console.log("dB Connection successful");
    const db = client.db(dbName);
    let collection = db.collection('customers');
    
    pipeArray.forEach((pipe, index)=>{
        tasks.push(function(){
            console.log('Processing items in pipe ' + (index+1) + ' out of ' + (customer.length/limit));
            collection.insertMany(pipe,(err)=>{
                if(err) throw err;
            });
        });
    });

    console.log('Launching ' + tasks.length + ' parallel tasks');
    const startTime = Date.now();
    async.parallel(tasks);
    const endTime = Date.now();
    console.log('Execution time: ' + (endTime - startTime));
    client.close();
});
