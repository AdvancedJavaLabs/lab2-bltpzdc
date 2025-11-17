#include <algorithm>
#include <cstddef>
#include <iostream>
#include <pqxx/connection.hxx>
#include <string>
#include <signal.h>

#include "constants.hpp"
#include "handlers.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

static volatile sig_atomic_t run = 1;

void stop(int sig) {
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
    
    if ( not rmq.startConsuming(QUEUE_NAME) ) {
        std::cerr << "Error: Cannot start consuming" << std::endl;
        return 1;
    }
    
    std::cout << "Worker started. Subscribed to queue: " << QUEUE_NAME << std::endl;
    std::cout << "Waiting for tasks..." << std::endl;
    
    while ( run ) {
        std::string message;
        
        if ( rmq.receiveMessage(message, 1) ) {
            std::cout << "Received message: " << message << std::endl;
            messages::TaskMessage task = messages::TaskMessage::fromJson(message);
            
            std::cout << "----------------------------------------" << std::endl;
            std::cout << "Received task:" << std::endl;
            std::cout << "  Task ID: " << task.taskId << std::endl;
            std::cout << "  Task type: " << messages::TaskMessage::typeToString(task.type) << std::endl;
            std::cout << "  Section IDs: [";
            for (size_t i = 0; i < task.sectionIds.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << task.sectionIds[i];
            }
            std::cout << "]" << std::endl;
            std::cout << "  Total sections in task: " << task.totalSections << std::endl;
            std::cout << "  Batch size: " << task.sectionIds.size() << std::endl;
            if ( task.n ) {
                std::cout << "  N: " << *task.n << std::endl;
            }
            std::cout << "----------------------------------------" << std::endl;
            if ( auto handler = handlers::handlers.find(task.type); handler != handlers::handlers.end() ) {
                auto result = (handler->second)(conn, task);
                result.totalSections = task.totalSections;
                
                if ( result.type == messages::Type::WordsCount ) {
                    std::cout << "Word count: " << std::get<size_t>(result.result) << std::endl;
                } else if ( result.type == messages::Type::TopN ) {
                    auto& topWords = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
                    std::cout << "Top " << topWords.size() << " words:" << std::endl;
                    for ( const auto& [count, word] : topWords ) {
                        std::cout << "  " << word << ": " << count << std::endl;
                    }
                } else if ( result.type == messages::Type::SortSentences ) {
                    std::cout << "Sorted sentences (by length):" << std::endl;
                    auto sentences = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
                    for ( const auto& sentence : sentences ) {
                        std::cout << sentence.first << " " << sentence.second << std::endl;
                    }
                }
            } else {
                std::cerr << "Error: can't find handler for task with type " << messages::TaskMessage::typeToString(task.type) << std::endl;
            }
        }
    }
    
    std::cout << "Shutting down worker..." << std::endl;
    
    return 0;
}
