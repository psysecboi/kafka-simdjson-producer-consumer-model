#include <iostream>
#include <string>
#include <vector>
#include <librdkafka/rdkafkacpp.h>
#include "simdjson.h"

int main(){
    std::string brokers = "localhost:9092";
    std::string group_id = "my_group";
    std::string topic_name = "test_topic";
    std::string errstr;

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", group_id, errstr);
    conf->set("enable.auto.commit", "true", errstr);
    conf->set("auto.offset.reset", "earliest", errstr); 

    RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    // if(!consumer){
    //     std::cerr << "Failed to create consumer: " << errstr << std::endl;
    //     return 1;
    // }

    std::vector<std::string> topics = {topic_name};
    RdKafka::ErrorCode resp = consumer->subscribe(topics);
    if(resp != RdKafka::ERR_NO_ERROR){
        std::cerr << "Failed to subscribe: " << RdKafka::err2str(resp) << std::endl;
        return 1;
    }

    simdjson::ondemand::parser parser;

    while(true){
        RdKafka::Message* msg = consumer->consume(5000);  
        if(!msg){
            std::cout<<"No message pointer received"<<std::endl;
            continue;
        }

        // std::cout<<"Received message with error code: "<<msg->err()<<std::endl;

        if(msg->err() == RdKafka::ERR_NO_ERROR){
            std::string json_str(static_cast<const char*>(msg->payload()), msg->len());

            auto doc = parser.iterate(json_str);

            auto key_result = doc["Name"].get_string();
            auto value_result = doc["Age"].get_double();

            if(key_result.error()){
                std::cerr<<"Error parsing Name field: "<<key_result.error()<<std::endl;
                delete msg;
                continue;
            }
            if(value_result.error()){
                std::cerr<<"Error parsing Age field: "<<value_result.error()<<std::endl;
                delete msg;
                continue;
            }

            std::string name = std::string(key_result.value());
            double value = value_result.value();

            std::cout<<"Received message: Name= "<<name<<", Age= "<<value<<std::endl;
        }
        else if(msg->err() == RdKafka::ERR__TIMED_OUT){
            delete msg;
            continue;
        }
        else std::cerr<<"Error: "<<msg->errstr()<<std::endl;

        delete msg;
    }

    consumer->close();
    delete consumer;
    delete conf;

    return 0;
}
