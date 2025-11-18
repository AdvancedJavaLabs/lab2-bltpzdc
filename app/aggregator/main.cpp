#include <iostream>
#include <csignal>
#include <algorithm>
#include <sstream>

#include "aggregators.hpp"
#include "constants.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

static volatile int run = 1;

static void stop(int sig) {
    run = 0;
}

static std::optional<messages::ResultMessage> aggregateResults(std::vector<messages::ResultMessage>& results)
{
    if ( results.empty() ) { return {}; }

    auto type = results[0].type;
    if ( auto aggregator = aggregators::aggregators.find(type); aggregator != aggregators::aggregators.end() ) {
        auto result = (aggregator->second)(results);
        result.taskId = results[0].taskId;
        result.type = results[0].type;
        result.totalSections = results[0].totalSections;
        result.sectionsCount = 0;

        for ( const auto& r : results ) {
            result.sectionsCount += r.sectionsCount;
        }

        return result;
    } else {
        std::cerr << "Error: can't find aggregator for type " << messages::typeToString(type);
        return {};
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, stop);
    signal(SIGTERM, stop);

    RabbitMQ rmq;

    if ( not rmq.connect(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD) ) {
        std::cerr << "Error: Cannot connect to RabbitMQ" << std::endl;
        return 1;
    }

    if ( not rmq.declareQueue(RESULTS_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare results queue" << std::endl;
        return 1;
    }

    if ( not rmq.declareQueue(SINKER_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare sinker queue" << std::endl;
        return 1;
    }

    if ( not rmq.startConsuming(RESULTS_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot start consuming" << std::endl;
        return 1;
    }

    std::cout << "Aggregator started." << std::endl;

    std::unordered_map<int, std::vector<messages::ResultMessage>> taskResults;

    while ( run ) {
        std::string message;

        if ( rmq.receiveMessage(message, 1) ) {
            auto result = messages::ResultMessage::fromJson(message);

            taskResults[result.taskId].push_back(result);

            int totalSectionsReceived = 0;
            for ( const auto& r : taskResults[result.taskId] ) {
                totalSectionsReceived += r.sectionsCount;
            }

            if ( totalSectionsReceived >= result.totalSections ) {
                auto aggregated = aggregateResults(taskResults[result.taskId]);
                if ( aggregated ) {
                    auto resultJson = aggregated->toJson();
                    rmq.sendMessage(resultJson, SINKER_QUEUE_NAME);
                }

                taskResults.erase(result.taskId);
            }
        }
    }

    std::cout << "Shutting down aggregator..." << std::endl;
    return 0;
}

