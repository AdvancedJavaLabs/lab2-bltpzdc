#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <sstream>
#include <unordered_map>

namespace messages {

enum class Type : uint8_t {
    WordsCount = 0,
    TopN,
    Tonality,
    SortSentences,
    Unknown,
};

struct TaskMessage {
    static inline const std::unordered_map<Type, std::string> stringTypes = {
        {Type::WordsCount, "words_count"},
        {Type::TopN, "top_n"},
        {Type::Tonality, "tonality"},
        {Type::SortSentences, "sort_sentences"},
    };

    static inline const std::unordered_map<std::string, Type> typeStrings = {
        { "words_count", Type::WordsCount},
        { "top_n", Type::TopN},
        { "tonality", Type::Tonality},
        { "sort_sentences", Type::SortSentences},
    };

    static std::string typeToString(Type type) {
        return stringTypes.find(type) != stringTypes.end() ? stringTypes.at(type) : "unknown";
    }

    static Type stringToType(const std::string& str) {
        return typeStrings.find(str) != typeStrings.end() ? typeStrings.at(str) : Type::Unknown;
    }

    int taskId{0};
    Type type{Type::Unknown};
    std::vector<int> sectionIds{};
    int totalSections{0};
    std::optional<int64_t> n{};

    std::string toJson() const
    {
        std::ostringstream json;
        json << "{";
        json << "\"task_id\":" << taskId << ",";
        json << "\"type\":\"" << typeToString(type) << "\",";
        json << "\"section_ids\":[";
        for ( size_t i = 0; i < sectionIds.size(); ++i ) {
            if (i > 0) { json << ","; }
            json << sectionIds[i];
        }
        json << "],";
        json << "\"total_sections\":" << totalSections;
        if ( n ) {
            json << ",";
            json << "\"n\":" << *n;
        }
        json << "}";
        return json.str();
    }
    
    static TaskMessage fromJson(const std::string& json)
    {
        TaskMessage task;
        
        size_t taskIdPos = json.find("\"task_id\":");
        if ( taskIdPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", taskIdPos);
            size_t end = json.find_first_not_of("0123456789", start);
            task.taskId = std::stoi(json.substr(start, end - start));
        }

        size_t typePos = json.find("\"type\":");
        if ( typePos != std::string::npos ) {
            size_t valueStart = json.find('\"', typePos + 7);
            if ( valueStart != std::string::npos ) {
                size_t valueEnd = json.find('\"', valueStart + 1);
                if ( valueEnd != std::string::npos ) {
                    std::string typeStr = json.substr(valueStart + 1, valueEnd - valueStart - 1);
                    task.type = stringToType(typeStr);
                }
            }
        }
        
        size_t sectionIdsPos = json.find("\"section_ids\":[");
        if ( sectionIdsPos != std::string::npos ) {
            size_t start = sectionIdsPos + 15;
            size_t end = json.find("]", start);
            std::string idsStr = json.substr(start, end - start);
            
            std::istringstream iss(idsStr);
            std::string id;
            while ( std::getline(iss, id, ',') ) {
                id.erase(0, id.find_first_not_of(" \t"));
                id.erase(id.find_last_not_of(" \t") + 1);
                if ( not id.empty() ) { task.sectionIds.push_back(std::stoi(id)); }
            }
        }
        
        size_t totalSectionsPos = json.find("\"total_sections\":");
        if ( totalSectionsPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", totalSectionsPos);
            size_t end = json.find_first_not_of("0123456789", start);
            task.totalSections = std::stoi(json.substr(start, end - start));
        }

        size_t nPos = json.find("\"n\":");
        if ( nPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", nPos);
            size_t end = json.find_first_not_of("0123456789", start);
            task.n = std::stoi(json.substr(start, end - start));
        }
        
        return task;
    }
};

struct ResultMessage {
    using result_t = std::variant<size_t, std::vector<std::pair<size_t, std::string>>, std::string>;

    Type type{Type::Unknown};
    result_t result{};
    int sectionsCount{0};
    int totalSections{0};
};

}
