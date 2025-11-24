#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <csignal>
#include <ostream>
#include <sstream>

#include "constants.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

namespace fs = std::filesystem;

static volatile int run = 1;

static void stop(int sig) {
    run = 0;
}

static void formatResultForFile(std::ofstream& file, const messages::ResultMessage& result)
{
    file << "========================================" << std::endl;
    file << "AGGREGATED RESULT FOR TASK " << result.taskId << std::endl;
    file << "Sections processed: " << result.totalSections << " / " << result.totalSections << std::endl;
    file << "----------------------------------------" << std::endl;

    file << "Words count: " << result.wordsCount << std::endl;
    file << std::endl;

    file << "Top 1000 words:" << std::endl;
    for ( const auto nw : result.topWords ) {
        file << nw.second << ": " << nw.first << std::endl; 
    }
    file << std::endl;

    file << "Sorted sentences:" << std::endl;
    for ( const auto& sent : result.sortedSentences ) {
        file << sent.second << " (" << sent.first << ")" << std::endl;
    }
    file << std::endl;

    std::string tonalityStr = "neutral";
    if ( result.tonality > 0 ) { tonalityStr = "positive"; }
    else if ( result.tonality < 0 ) { tonalityStr = "negative"; }
    file << "Tonality: " <<  tonalityStr << std::endl;
    file << std::endl;

    file << "Text with replacements: " << result.replacedText << std::endl;

    file << "========================================" << std::endl;
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
            if ( std::ofstream file(filePath); file.is_open() ) { formatResultForFile(file, result); }
        }
    }

    std::cout << "Shutting down sinker..." << std::endl;
    return 0;
}

