#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

int main() {
    std::string brokers = "localhost:9092";
    std::string topic_str = "test_topic";
    std::string errstr;

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);

    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
    // if(!producer){
    //     std::cerr << "Failed to create producer: " << errstr << std::endl;
    //     return 1;
    // }

    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    RdKafka::Topic* topic = RdKafka::Topic::create(producer, topic_str, tconf, errstr);
    // if(!topic){
    //     std::cerr << "Failed to create topic: " << errstr << std::endl;
    //     return 1;
    // }

    std::string json_message = R"({"Name": "Toms", "Age": 19})";

    RdKafka::ErrorCode resp = producer->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(json_message.c_str()),
        json_message.size(),
        nullptr, 
        nullptr
    );

    if(resp != RdKafka::ERR_NO_ERROR){
        std::cerr<< "Produce failed: "<<RdKafka::err2str(resp)<<std::endl;
    } else {
        std::cout<<"Message produced: "<<json_message<<std::endl;
    }

    producer->flush(5000);

    delete topic;
    delete tconf;
    delete producer;
    delete conf;

    return 0;
}
