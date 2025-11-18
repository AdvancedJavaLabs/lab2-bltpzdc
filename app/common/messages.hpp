#pragma once

#include <optional>
#include <variant>
#include <vector>
#include <unordered_map>
#include <string>

namespace messages {

enum class Type : uint8_t {
    WordsCount = 0,
    TopN,
    Tonality,
    SortSentences,
    Unknown,
};

inline const std::unordered_map<Type, std::string> stringTypes = {
        {Type::WordsCount, "words_count"},
        {Type::TopN, "top_n"},
        {Type::Tonality, "tonality"},
        {Type::SortSentences, "sort_sentences"},
};

inline const std::unordered_map<std::string, Type> typeStrings = {
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

struct TaskMessage {
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

    int taskId{0};
    Type type{Type::Unknown};
    result_t result{};
    int sectionsCount{0};
    int totalSections{0};
    std::optional<int64_t> n{};

    static std::string escapeJsonString(const std::string& str) {
        std::string escaped;
        escaped.reserve(str.length() + 10);
        for ( char c : str ) {
            if ( c == '"' ) {
                escaped += "\\\"";
            } else if ( c == '\\' ) {
                escaped += "\\\\";
            } else if ( c == '\n' ) {
                escaped += "\\n";
            } else if ( c == '\r' ) {
                escaped += "\\r";
            } else if ( c == '\t' ) {
                escaped += "\\t";
            } else {
                escaped += c;
            }
        }
        return escaped;
    }

    static std::string unescapeJsonString(const std::string& str) {
        std::string unescaped;
        unescaped.reserve(str.length());
        for ( size_t i = 0; i < str.length(); ++i ) {
            if ( str[i] == '\\' and i + 1 < str.length() ) {
                switch ( str[i + 1] ) {
                    case '"': unescaped += '"'; ++i; break;
                    case '\\': unescaped += '\\'; ++i; break;
                    case 'n': unescaped += '\n'; ++i; break;
                    case 'r': unescaped += '\r'; ++i; break;
                    case 't': unescaped += '\t'; ++i; break;
                    default: unescaped += str[i]; break;
                }
            } else {
                unescaped += str[i];
            }
        }
        return unescaped;
    }

    static size_t findJsonStringEnd(const std::string& json, size_t start) {
        for ( size_t i = start; i < json.length(); ++i ) {
            if ( json[i] == '"' ) {
                size_t backslashCount = 0;
                size_t j = i - 1;
                while ( j >= start and json[j] == '\\' ) {
                    ++backslashCount;
                    --j;
                }
                if ( backslashCount % 2 == 0 ) { return i; }
            }
        }
        return std::string::npos;
    }

    std::string toJson() const
    {
        std::ostringstream json;
        json << "{";
        json << "\"task_id\":" << taskId << ",";
        json << "\"type\":\"" << typeToString(type) << "\",";
        json << "\"sections_count\":" << sectionsCount << ",";
        json << "\"total_sections\":" << totalSections;
        if ( n ) {
            json << ",\"n\":" << *n;
        }
        json << ",\"result\":";
        
        if ( type == Type::WordsCount ) {
            json << std::get<size_t>(result);
        } else if ( type == Type::TopN ) {
            json << "[";
            const auto& topWords = std::get<std::vector<std::pair<size_t, std::string>>>(result);
            for ( size_t i = 0; i < topWords.size(); ++i ) {
                if ( i > 0 ) json << ",";
                json << "{\"word\":\"" << escapeJsonString(topWords[i].second) << "\",\"count\":" << topWords[i].first << "}";
            }
            json << "]";
        } else if ( type == Type::SortSentences ) {
            const auto& sentences = std::get<std::vector<std::pair<size_t, std::string>>>(result);
            json << "[";
            for ( size_t i = 0; i < sentences.size(); ++i ) {
                if ( i > 0 ) json << ",";
                json << "{\"length\":" << sentences[i].first << ",\"sentence\":\"" << escapeJsonString(sentences[i].second) << "\"}";
            }
            json << "]";
        } else if ( type == Type::Tonality ) {
            json << "\"" << escapeJsonString(std::get<std::string>(result)) << "\"";
        } else {
            json << "null";
        }
        
        json << "}";
        return json.str();
    }

    static ResultMessage fromJson(const std::string& json)
    {
        ResultMessage result;
        
        size_t taskIdPos = json.find("\"task_id\":");
        if ( taskIdPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", taskIdPos);
            size_t end = json.find_first_not_of("0123456789", start);
            result.taskId = std::stoi(json.substr(start, end - start));
        }

        size_t typePos = json.find("\"type\":");
        if ( typePos != std::string::npos ) {
            size_t valueStart = json.find('\"', typePos + 7);
            if ( valueStart != std::string::npos ) {
                size_t valueEnd = json.find('\"', valueStart + 1);
                if ( valueEnd != std::string::npos ) {
                    std::string typeStr = json.substr(valueStart + 1, valueEnd - valueStart - 1);
                    result.type = stringToType(typeStr);
                }
            }
        }

        size_t sectionsCountPos = json.find("\"sections_count\":");
        if ( sectionsCountPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", sectionsCountPos);
            size_t end = json.find_first_not_of("0123456789", start);
            result.sectionsCount = std::stoi(json.substr(start, end - start));
        }

        size_t totalSectionsPos = json.find("\"total_sections\":");
        if ( totalSectionsPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", totalSectionsPos);
            size_t end = json.find_first_not_of("0123456789", start);
            result.totalSections = std::stoi(json.substr(start, end - start));
        }

        size_t nPos = json.find("\"n\":");
        if ( nPos != std::string::npos ) {
            size_t start = json.find_first_of("0123456789", nPos);
            size_t end = json.find_first_not_of("0123456789", start);
            result.n = std::stoi(json.substr(start, end - start));
        }

        size_t resultPos = json.find("\"result\":");
        if ( resultPos != std::string::npos ) {
            if ( result.type == Type::WordsCount ) {
                size_t start = json.find_first_of("0123456789", resultPos);
                size_t end = json.find_first_not_of("0123456789", start);
                result.result = std::stoull(json.substr(start, end - start));
            } else if ( result.type == Type::TopN ) {
                std::vector<std::pair<size_t, std::string>> topWords;
                size_t arrayStart = json.find('[', resultPos);
                if ( arrayStart != std::string::npos ) {
                    size_t pos = arrayStart + 1;
                    while ( pos < json.length() ) {
                        size_t wordStart = json.find("\"word\":\"", pos);
                        if ( wordStart == std::string::npos ) break;
                        wordStart += 8;
                        size_t wordEnd = findJsonStringEnd(json, wordStart);
                        if ( wordEnd == std::string::npos ) break;
                        std::string word = json.substr(wordStart, wordEnd - wordStart);
                        word = unescapeJsonString(word);
                        
                        size_t countStart = json.find("\"count\":", wordEnd);
                        countStart = json.find_first_of("0123456789", countStart);
                        size_t countEnd = json.find_first_not_of("0123456789", countStart);
                        size_t count = std::stoull(json.substr(countStart, countEnd - countStart));
                        
                        topWords.emplace_back(count, word);
                        pos = json.find('}', countEnd);
                        if ( pos == std::string::npos ) break;
                        pos++;
                        if ( json[pos] == ']' ) break;
                    }
                }
                result.result = topWords;
            } else if ( result.type == Type::SortSentences ) {
                std::vector<std::pair<size_t, std::string>> sentences;
                size_t arrayStart = json.find('[', resultPos);
                if ( arrayStart != std::string::npos ) {
                    size_t pos = arrayStart + 1;
                    while ( pos < json.length() ) {
                        size_t lengthStart = json.find("\"length\":", pos);
                        if ( lengthStart == std::string::npos ) break;
                        lengthStart = json.find_first_of("0123456789", lengthStart);
                        size_t lengthEnd = json.find_first_not_of("0123456789", lengthStart);
                        size_t length = std::stoull(json.substr(lengthStart, lengthEnd - lengthStart));
                        
                        size_t sentenceStart = json.find("\"sentence\":\"", lengthEnd);
                        if ( sentenceStart == std::string::npos ) break;
                        sentenceStart += 12;
                        size_t sentenceEnd = findJsonStringEnd(json, sentenceStart);
                        if ( sentenceEnd == std::string::npos ) break;
                        std::string sentence = json.substr(sentenceStart, sentenceEnd - sentenceStart);
                        sentence = unescapeJsonString(sentence);
                        
                        sentences.emplace_back(length, sentence);
                        pos = json.find('}', sentenceEnd);
                        if ( pos == std::string::npos ) break;
                        pos++;
                        if ( json[pos] == ']' ) break;
                    }
                }
                result.result = sentences;
            } else if ( result.type == Type::Tonality ) {
                size_t stringStart = json.find('"', resultPos);
                if ( stringStart != std::string::npos ) {
                    stringStart += 1;
                    size_t stringEnd = findJsonStringEnd(json, stringStart);
                    if ( stringEnd != std::string::npos ) {
                        std::string tonalityStr = json.substr(stringStart, stringEnd - stringStart);
                        result.result = unescapeJsonString(tonalityStr);
                    }
                }
            }
        }
        
        return result;
    }
};

}
