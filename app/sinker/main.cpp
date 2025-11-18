#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <csignal>
#include <sstream>

#include "constants.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

namespace fs = std::filesystem;

static volatile int run = 1;

static void stop(int sig) {
    run = 0;
}

static std::string formatResultForFile(const messages::ResultMessage& result)
{
    std::ostringstream output;
    output << "========================================" << std::endl;
    output << "AGGREGATED RESULT FOR TASK " << result.taskId << std::endl;
    output << "Type: " << messages::typeToString(result.type) << std::endl;
    output << "Sections processed: " << result.sectionsCount << " / " << result.totalSections << std::endl;
    output << "----------------------------------------" << std::endl;

    if ( result.type == messages::Type::WordsCount ) {
        output << "Total word count: " << std::get<size_t>(result.result) << std::endl;
    } else if ( result.type == messages::Type::TopN ) {
        const auto& topWords = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
        output << "Top " << topWords.size() << " words:" << std::endl;
        for ( const auto& [count, word] : topWords ) {
            output << "  " << word << ": " << count << std::endl;
        }
    } else if ( result.type == messages::Type::Tonality ) {
        output << "Tonality: " << std::get<std::string>(result.result) << std::endl;
    } else if ( result.type == messages::Type::SortSentences ) {
        const auto& sentences = std::get<std::vector<std::pair<size_t, std::string>>>(result.result);
        output << "Sorted sentences (by length, descending):" << std::endl;
        for ( const auto& [length, sentence] : sentences ) {
            output << "  [" << length << "] " << sentence << std::endl;
        }
    }

    output << "========================================" << std::endl;
    return output.str();
}

int main(int argc, char* argv[]) {
    signal(SIGINT, stop);
    signal(SIGTERM, stop);

    fs::path resultsDir = "results";
    if ( not fs::exists(resultsDir) ) { fs::create_directories(resultsDir); }

    RabbitMQ rmq;
    if ( not rmq.connect(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD) ) {
        std::cerr << "Error: Cannot connect to RabbitMQ" << std::endl;
        return 1;
    }

    if ( not rmq.declareQueue(SINKER_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare sinker queue" << std::endl;
        return 1;
    }

    if ( not rmq.startConsuming(SINKER_QUEUE_NAME) ) {
        std::cerr << "Error: Cannot start consuming" << std::endl;
        return 1;
    }

    std::cout << "Sinker started." << std::endl;

    while ( run ) {
        std::string message;

        if ( rmq.receiveMessage(message, 1) ) {
            auto result = messages::ResultMessage::fromJson(message);
            
            auto endTime = std::chrono::system_clock::now();
            auto timeT = std::chrono::system_clock::to_time_t(endTime);
            auto* tmPtr = std::localtime(&timeT);
            
            std::ostringstream timeStr;
            timeStr << std::put_time(tmPtr, "%Y-%m-%d %H:%M:%S");
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                endTime.time_since_epoch()) % 1000;
            timeStr << "." << std::setfill('0') << std::setw(3) << ms.count();
            
            std::cout << "[TASK END] Task " << result.taskId << " completed at " << timeStr.str() << std::endl;

            fs::path filePath = resultsDir / ("task_" + std::to_string(result.taskId) + ".txt");
            if ( std::ofstream file(filePath); file.is_open() ) { file << formatResultForFile(result); }
        }
    }

    std::cout << "Shutting down sinker..." << std::endl;
    return 0;
}

