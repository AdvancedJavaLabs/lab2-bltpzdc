#include <cstdint>
#include <iostream>
#include <optional>
#include <pqxx/pqxx>
#include <string>

#include "constants.hpp"
#include "messages.hpp"
#include "rabbitmq.hpp"

static int taskIdCounter = 1;

void listTexts(pqxx::connection& conn, RabbitMQ&, const std::string&, std::istringstream&) {
    pqxx::work txn(conn);
    
    auto result = txn.exec("SELECT name FROM texts ORDER BY name");
    for ( const auto& row : result ) {
        std::cout << "  - " << row[0].as<std::string>() << std::endl;
    }
}

std::vector<int> getSectionIds(pqxx::connection& conn, const std::string& textName) {
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

void createTask(pqxx::connection& dbConn, RabbitMQ& rmq, const std::string& strCmd, std::istringstream& iss) {
    int taskId = taskIdCounter++;
    auto taskType = messages::TaskMessage::stringToType(strCmd);

    std::optional<int64_t> n;
    if ( taskType == messages::Type::TopN ) {
        if (std::string strN; (iss >> strN) ) {
            n = std::stoi(strN);
        }
    }

    std::string textName;
    if ( not (iss >> textName) ) {
        std::cerr << "Error: Text name is required" << std::endl;
        return;
    }

    auto sectionIds = getSectionIds(dbConn, textName);
    
    if ( sectionIds.empty()) {
        std::cerr << "No sections found for text: " << textName << std::endl;
        return;
    }
    
    int totalSections = sectionIds.size();
    
    std::cout << "Starting task " << taskId << " for text: " << textName << std::endl;
    std::cout << "Total sections: " << totalSections << std::endl;
    
    for ( size_t i = 0; i < sectionIds.size(); i += BATCH_SIZE ) {
        messages::TaskMessage msg;
        msg.taskId = taskId;
        msg.type = taskType;
        msg.totalSections = totalSections;
        msg.n = n;
        
        for ( size_t j = i; j < i + BATCH_SIZE && j < sectionIds.size(); ++j ) {
            msg.sectionIds.push_back(sectionIds[j]);
        }
        
        if ( rmq.sendMessage(msg.toJson(), QUEUE_NAME) ) {
            std::cout << "Sent batch: " << msg.sectionIds.size() << " sections (IDs: ";
            for (size_t k = 0; k < msg.sectionIds.size(); ++k) {
                if (k > 0) std::cout << ", ";
                std::cout << msg.sectionIds[k];
            }
            std::cout << ")" << std::endl;
        } else {
            std::cerr << "Failed to send batch to RabbitMQ" << std::endl;
        }
    }
    
    std::cout << "Task " << taskId << " completed" << std::endl;
}

void printUsage() {
    std::cout << "Usage:" << std::endl;
    std::cout << "  list                    - List all texts" << std::endl;
    std::cout << "  words_count <text_name> - Count words in a text" << std::endl;
    std::cout << "  top_n <text_name>       - Get top N words in a text" << std::endl;
    std::cout << "  tonality <text_name>    - Get tonality of a text" << std::endl;
    std::cout << "  sort_sentences <text_name> - Sort sentences in a text" << std::endl;
}

using Command = void(*)(pqxx::connection&, RabbitMQ&, const std::string&, std::istringstream&);
static const std::unordered_map<std::string, Command> commands = {
    {"list", listTexts},
    {"words_count", createTask},
    {"top_n", createTask},
    {"tonality", createTask},
    {"sort_sentences", createTask},
};

int main() {
    pqxx::connection conn(DB_CONN_STRING);
    if ( not conn.is_open() ) {
        std::cerr << "Error: Cannot connect to database" << std::endl;
        return 1;
    }
    
    std::cout << "Connected to PostgreSQL database" << std::endl;
    
    RabbitMQ rmq;
    if ( not rmq.connect(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD) ) {
        std::cerr << "Error: Cannot connect to RabbitMQ" << std::endl;
        return 1;
    }
    
    if ( not rmq.declareQueue(QUEUE_NAME) ) {
        std::cerr << "Error: Cannot declare queue" << std::endl;
        return 1;
    }
    
    std::cout << "Connected to RabbitMQ broker" << std::endl;

    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if ( auto it = commands.find(cmd); it != commands.end() ) {
            it->second(conn, rmq, cmd, iss);
        } else {
            printUsage();
        }
    }
    
    return 0;
}
