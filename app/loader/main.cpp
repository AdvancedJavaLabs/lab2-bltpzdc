#include <iostream>
#include <fstream>
#include <filesystem>
#include <pqxx/pqxx>
#include <algorithm>

#include "constants.hpp"

namespace fs = std::filesystem;

std::string sanitizeUTF8(const std::string& input) {
    std::string output;
    output.reserve(input.size());
    
    for (size_t i = 0; i < input.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(input[i]);
        
        // ASCII character (0x00-0x7F)
        if (c <= 0x7F) {
            output += c;
        }
        // Start of 2-byte UTF-8 sequence (0xC2-0xDF)
        else if (c >= 0xC2 && c <= 0xDF) {
            if (i + 1 < input.size()) {
                unsigned char c2 = static_cast<unsigned char>(input[i + 1]);
                if (c2 >= 0x80 && c2 <= 0xBF) {
                    output += c;
                    output += c2;
                    ++i;
                } else {
                    // Invalid continuation byte, replace with space
                    output += ' ';
                }
            } else {
                // Incomplete sequence, replace with space
                output += ' ';
            }
        }
        // Start of 3-byte UTF-8 sequence (0xE0-0xEF)
        else if (c >= 0xE0 && c <= 0xEF) {
            if (i + 2 < input.size()) {
                unsigned char c2 = static_cast<unsigned char>(input[i + 1]);
                unsigned char c3 = static_cast<unsigned char>(input[i + 2]);
                if (c2 >= 0x80 && c2 <= 0xBF && c3 >= 0x80 && c3 <= 0xBF) {
                    // Check for overlong encoding
                    if (!(c == 0xE0 && c2 < 0xA0) && !(c == 0xED && c2 > 0x9F)) {
                        output += c;
                        output += c2;
                        output += c3;
                        i += 2;
                    } else {
                        output += ' ';
                    }
                } else {
                    // Invalid continuation bytes, replace with space
                    output += ' ';
                }
            } else {
                // Incomplete sequence, replace with space
                output += ' ';
            }
        }
        // Start of 4-byte UTF-8 sequence (0xF0-0xF4)
        else if (c >= 0xF0 && c <= 0xF4) {
            if (i + 3 < input.size()) {
                unsigned char c2 = static_cast<unsigned char>(input[i + 1]);
                unsigned char c3 = static_cast<unsigned char>(input[i + 2]);
                unsigned char c4 = static_cast<unsigned char>(input[i + 3]);
                if (c2 >= 0x80 && c2 <= 0xBF && c3 >= 0x80 && c3 <= 0xBF && 
                    c4 >= 0x80 && c4 <= 0xBF) {
                    // Check for overlong encoding and invalid ranges
                    if (!(c == 0xF0 && c2 < 0x90) && !(c == 0xF4 && c2 > 0x8F)) {
                        output += c;
                        output += c2;
                        output += c3;
                        output += c4;
                        i += 3;
                    } else {
                        output += ' ';
                    }
                } else {
                    // Invalid continuation bytes, replace with space
                    output += ' ';
                }
            } else {
                // Incomplete sequence, replace with space
                output += ' ';
            }
        }
        // Invalid UTF-8 byte (0x80-0xBF without leading byte, 0xC0-0xC1, 0xF5-0xFF)
        else {
            // Replace invalid bytes with space
            output += ' ';
        }
    }
    
    return output;
}

// Check if a byte is a continuation byte in UTF-8
inline bool isUTF8Continuation(unsigned char c) {
    return (c >= 0x80 && c <= 0xBF);
}

// Find the start of the current UTF-8 character by going backwards
size_t findUTF8CharStart(const std::string& str, size_t pos) {
    if (pos == 0) return 0;
    
    // Go backwards until we find a non-continuation byte
    size_t start = pos;
    while (start > 0 && isUTF8Continuation(static_cast<unsigned char>(str[start]))) {
        --start;
    }
    
    // Check if this is a valid UTF-8 start byte
    unsigned char c = static_cast<unsigned char>(str[start]);
    if (c <= 0x7F) {
        // ASCII
        return start;
    } else if (c >= 0xC2 && c <= 0xDF) {
        // 2-byte sequence
        return start;
    } else if (c >= 0xE0 && c <= 0xEF) {
        // 3-byte sequence
        return start;
    } else if (c >= 0xF0 && c <= 0xF4) {
        // 4-byte sequence
        return start;
    }
    
    // Invalid byte, just return the position
    return pos;
}

std::vector<std::string> splitByChunks(const std::string& content, size_t chunkSize = 1024) {
    std::vector<std::string> chunks;
    
    for ( size_t i = 0; i < content.length(); ) {
        size_t end = std::min(i + chunkSize, content.length());
        
        // If we're not at the end of the string, make sure we don't split in the middle of a UTF-8 character
        if (end < content.length()) {
            // Check if the byte at 'end' is a continuation byte
            if (isUTF8Continuation(static_cast<unsigned char>(content[end]))) {
                // Find the start of this UTF-8 character and adjust 'end' to before it
                end = findUTF8CharStart(content, end);
            }
        }
        
        chunks.push_back(content.substr(i, end - i));
        i = end;
    }
    
    return chunks;
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
            
            // Sanitize UTF-8 to remove invalid sequences
            content = sanitizeUTF8(content);
            
            std::vector<std::string> sections = splitByChunks(content, 1024);
            
            insertTextAndSections(conn, textName, sections);
        } catch ( const std::exception& e ) {
            std::cerr << "Error loading '" << textName << "': " << e.what() << std::endl;
        }
    }
    
    return 0;
}

