#include <chrono>
#include <iomanip>
#include <iostream>
#include <optional>
#include <pqxx/pqxx>
#include <sstream>
#include <string>

#include "constants.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

static int taskIdCounter = 1;

static void listTexts(pqxx::connection& conn) {
    pqxx::work txn(conn);
    
    auto result = txn.exec("SELECT name FROM texts ORDER BY name");
    for ( const auto& row : result ) {
        std::cout << "  - " << row[0].as<std::string>() << std::endl;
    }
}

static std::vector<int> getSectionIds(pqxx::connection& conn, const std::string& textName) {
    std::vector<int> sectionIds;
    sectionIds.reserve(128);
    pqxx::work txn(conn);
    
    auto result = txn.exec_params(
        "SELECT s.id FROM sections s "
        "JOIN texts t ON s.text_id = t.id "
        "WHERE t.name = $1 "
        "ORDER BY s.section_number",
        textName
    );
    
    for ( const auto& row : result ) {
        sectionIds.push_back(row[0].as<int>());
    }
    
    return sectionIds;
}

static void createTask(pqxx::connection& dbConn, RabbitMQ& rmq, const std::string& textName) {
    int taskId = taskIdCounter++;
    auto sectionIds = getSectionIds(dbConn, textName);
    
    if ( sectionIds.empty()) {
        std::cerr << "No sections found for text: " << textName << std::endl;
        return;
    }
    
    int totalSections = sectionIds.size();
    
    auto startTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(startTime);
    auto* tmPtr = std::localtime(&timeT);
    
    std::ostringstream timeStr;
    timeStr << std::put_time(tmPtr, "%Y-%m-%d %H:%M:%S");
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        startTime.time_since_epoch()).count();
    timeStr << "." << std::setfill('0') << std::setw(3) << ms % 1'000;
    
    std::cout << "[TASK START] Task " << taskId << " started at " << timeStr.str() 
              << " for text: " << textName << std::endl;
    
    for ( size_t i = 0; i < sectionIds.size(); i += BATCH_SIZE ) {
        messages::TaskMessage msg;
        msg.taskId = taskId;
        msg.totalSections = totalSections;
        msg.startTime = ms;
        for ( size_t j = i; j < i + BATCH_SIZE and j < sectionIds.size(); ++j ) {
            msg.sectionIds.push_back(sectionIds[j]);
        }
        
        // std::cout << msg.toJson() << std::endl;
        rmq.sendMessage(msg.toJson(), QUEUE_NAME);
    }
}

static void printUsage() {
    std::cout << "Usage:" << std::endl;
    std::cout << "  list        - List all texts" << std::endl;
    std::cout << "  <text_name> - Start processing a text" << std::endl;
}

int main() {
    pqxx::connection conn(DB_CONN_STRING);
    if ( not conn.is_open() ) {
        std::cerr << "Error: Cannot connect to database" << std::endl;
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
    
    std::string line;
    while (std::cout << "> " and std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if ( cmd == "list" ) {
            listTexts(conn);
        } else {
            createTask(conn, rmq, cmd);
        }

        // if ( auto it = commands.find(cmd); it != commands.end() ) {
        //     it->second(conn, rmq, cmd);
        // } else {
        //     printUsage();
        // }
    }
    
    return 0;
}
