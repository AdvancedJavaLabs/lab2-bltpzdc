#include <iostream>
#include <fstream>
#include <filesystem>
#include <pqxx/pqxx>
#include <algorithm>

#include "constants.hpp"

namespace fs = std::filesystem;

std::vector<std::string> splitByParagraphs(const std::string& content) {
    std::vector<std::string> paragraphs;
    std::string current;
    
    for ( size_t i = 0; i < content.length(); ++i ) {
        if ( i < content.length() - 1 and content[i] == '\n' and content[i + 1] == '\n') {
            std::string trimmed = current;
            size_t first = trimmed.find_first_not_of(" \t\n\r");

            if ( first != std::string::npos ) {
                trimmed = trimmed.substr(first);
                size_t last = trimmed.find_last_not_of(" \t\n\r");
                if ( last != std::string::npos ) { trimmed = trimmed.substr(0, last + 1); }
                if ( not trimmed.empty() ) { paragraphs.push_back(trimmed); }
            }
            current.clear();
            ++i;
        } else {
            current += content[i];
        }
    }
    
    if ( not current.empty() ) {
        std::string trimmed = current;
        size_t first = trimmed.find_first_not_of(" \t\n\r");
        if ( first != std::string::npos ) {
            trimmed = trimmed.substr(first);
            size_t last = trimmed.find_last_not_of(" \t\n\r");
            if ( last != std::string::npos ) { trimmed = trimmed.substr(0, last + 1); }
            if ( not trimmed.empty() ) { paragraphs.push_back(trimmed); }
        }
    }
    
    return paragraphs;
}

std::string readTextFile(const std::string& filePath) {
    if ( std::ifstream file(filePath); file ) {
        return std::string(std::istreambuf_iterator<char>(file),
                       std::istreambuf_iterator<char>());
    } else {
        throw std::runtime_error("Cannot open file: " + filePath);
    }    
}

void insertTextAndSections(pqxx::connection& conn, const std::string& textName, 
                          const std::vector<std::string>& sections) {
    pqxx::work txn(conn);
    
    try {
        auto textResult = txn.exec_params(
            "INSERT INTO texts (name) VALUES ($1) RETURNING id",
            textName
        );
        
        if ( textResult.empty() ) { throw std::runtime_error("Failed to insert text: " + textName); }
        
        auto textId = textResult[0][0].as<int>();
        
        for ( size_t i = 0; i < sections.size(); ++i ) {
            txn.exec_params(
                "INSERT INTO sections (text_id, content, section_number) VALUES ($1, $2, $3)",
                textId,
                sections[i],
                static_cast<int>(i + 1)
            );
        }
        
        txn.commit();
    } catch (const std::exception& e) {
        txn.abort();
        throw;
    }
}

int main(int argc, char* argv[]) {
    std::string textsDir = "texts";
    
    if (argc > 1) {
        textsDir = argv[1];
    }
    
    if ( not fs::exists(textsDir) or not fs::is_directory(textsDir)) {
        std::cerr << "Error: " << textsDir << " directory does not exist" << std::endl;
        return 1;
    }
    
    pqxx::connection conn(DB_CONN_STRING);
    if ( not conn.is_open() ) {
        std::cerr << "Error: Cannot connect to database" << std::endl;
        return 1;
    }
    
    std::vector<fs::path> textFiles;
    for ( const auto& entry : fs::directory_iterator(textsDir) ) {
        if ( entry.is_regular_file() and entry.path().extension() == ".txt" ) { textFiles.push_back(entry.path()); }
    }
    
    if ( textFiles.empty() ) {
        std::cerr << "No .txt files found in " << textsDir << std::endl;
        return 1;
    }
    
    std::sort(textFiles.begin(), textFiles.end());
    
    for ( const auto& textFile : textFiles ) {
        std::string textName = textFile.stem().string();
        std::string filePath = textFile.string();
        
        try {
            std::string content = readTextFile(filePath);
            
            std::vector<std::string> sections = splitByParagraphs(content);
            
            insertTextAndSections(conn, textName, sections);
        } catch ( const std::exception& e ) {
            std::cerr << "Error loading '" << textName << "': " << e.what() << std::endl;
        }
    }
    
    return 0;
}

