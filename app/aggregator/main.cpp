#include <iostream>
#include <csignal>
#include <algorithm>

#include "aggregators.hpp"
#include "constants.hpp"
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
        std::cerr << "Error: can't find aggregator for type " << messages::TaskMessage::typeToString(type);
        return {};
    }
}

void printAggregatedResult(const messages::ResultMessage& result)
{
    std::cout << "========================================" << std::endl;
    std::cout << "AGGREGATED RESULT FOR TASK " << result.taskId << std::endl;
    std::cout << "Type: " << messages::TaskMessage::typeToString(result.type) << std::endl;
    std::cout << "Sections processed: " << result.sectionsCount << " / " << result.totalSections << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    if ( result.type == messages::Type::WordsCount ) {
        std::cout << "Total word count: " << std::get<size_t>(result.result) << std::endl;
    } else if ( result.type == messages::Type::TopN ) {
        const auto& topWords = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
        std::cout << "Top " << topWords.size() << " words:" << std::endl;
        for ( const auto& [count, word] : topWords ) {
            std::cout << "  " << word << ": " << count << std::endl;
        }
    } else if ( result.type == messages::Type::Tonality ) {
        std::cout << "Tonality: " << std::get<std::string>(result.result) << std::endl;
    } else if ( result.type == messages::Type::SortSentences ) {
        const auto& sentences = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
        std::cout << "Sorted sentences (by length, descending):" << std::endl;
        for ( const auto& [length, sentence] : sentences ) {
            std::cout << "  [" << length << "] " << sentence << std::endl;
        }
    }

    std::cout << "========================================" << std::endl;
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

    if ( not rmq.startConsuming(RESULTS_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot start consuming" << std::endl;
        return 1;
    }

    std::cout << "Aggregator started. Subscribed to queue: " << RESULTS_QUEUE_NAME << std::endl;
    std::cout << "Waiting for results..." << std::endl;

    std::unordered_map<int, std::vector<messages::ResultMessage>> taskResults;

    while ( run ) {
        std::string message;

        if ( rmq.receiveMessage(message, 1) ) {
            auto result = messages::ResultMessage::fromJson(message);
            
            std::cout << "Received result for task " << result.taskId 
                      << " (" << result.sectionsCount << " sections)" << std::endl;

            taskResults[result.taskId].push_back(result);

            int totalSectionsReceived = 0;
            for ( const auto& r : taskResults[result.taskId] ) {
                totalSectionsReceived += r.sectionsCount;
            }

            if ( totalSectionsReceived >= result.totalSections ) {
                auto aggregated = aggregateResults(taskResults[result.taskId]);
                if ( aggregated ) {
                    printAggregatedResult(*aggregated);
                }

                taskResults.erase(result.taskId);
            } else {
                std::cout << "  Progress: " << totalSectionsReceived 
                          << " / " << result.totalSections << " sections" << std::endl;
            }
        }
    }

    std::cout << "Shutting down aggregator..." << std::endl;
    return 0;
}

