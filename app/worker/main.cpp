#include <algorithm>
#include <cstddef>
#include <iostream>
#include <pqxx/connection.hxx>
#include <string>
#include <csignal>

#include "constants.hpp"
#include "handlers.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

static volatile int run = 1;

static void stop(int sig) {
    run = 0;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, stop);
    signal(SIGTERM, stop);

    pqxx::connection conn{DB_CONN_STRING};
    
    if ( not conn.is_open() ) {
        std::cerr << "Error: Cannot connect to PostgreSQL" << std::endl;
        return 1;
    }
    
    RabbitMQ rmq;
    
    if ( not rmq.connect(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD) ) {
        std::cerr << "Error: Cannot connect to RabbitMQ" << std::endl;
        return 1;
    }
    
    if ( not rmq.declareQueue(QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare queue" << std::endl;
        return 1;
    }
    
    if ( not rmq.declareQueue(RESULTS_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare results queue" << std::endl;
        return 1;
    }
    
    if ( not rmq.startConsuming(QUEUE_NAME) ) {
        std::cerr << "Error: Cannot start consuming" << std::endl;
        return 1;
    }
    
    std::cout << "Worker started." << std::endl;
    
    while ( run ) {
        std::string message;
        
        if ( rmq.receiveMessage(message, 1) ) {
            auto task = messages::TaskMessage::fromJson(message);
            // std::cout << "Got message with id=" << task.taskId << std::endl
            //     << "total_sections=" << task.totalSections << std::endl
            //     << "start_time=" << task.startTime << std::endl
            //     << "sectionIds=[";
            // for ( auto sectionId : task.sectionIds ) {
            //     std::cout << sectionId << ",";
            // }
            // std::cout << "]" << std::endl;
            auto sections = getAllSections(conn, task.sectionIds);
            messages::ResultMessage result;
            result.taskId = task.taskId;
            result.sectionsCount = sections.size();
            result.totalSections = task.totalSections;
            result.startTime = task.startTime; 

            for ( auto handler : handlers::handlers ) {
                handler(sections, result);
            }
            // std::cout << result.toJson() << std::endl;
            rmq.sendMessage(result.toJson(), RESULTS_QUEUE_NAME);
        }
    }
    
    std::cout << "Shutting down worker..." << std::endl;
    return 0;
}
