#pragma once

#include "messages.hpp"
#include "constants.hpp"
#include <pqxx/connection.hxx>
#include <pqxx/pqxx>

#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <unordered_set>

inline std::vector<std::string> getAllSections(pqxx::connection& conn, const std::vector<int>& sectionIds)
{
    if ( sectionIds.empty() ) {
        return {};
    }
    
    pqxx::work txn(conn);
    
    std::ostringstream query;
    query << "SELECT content FROM sections WHERE id = ANY($1::int[]) ORDER BY id";
    
    std::string arrayStr = "{";
    for ( size_t i = 0; i < sectionIds.size(); ++i ) {
        if ( i > 0 ) arrayStr += ",";
        arrayStr += std::to_string(sectionIds[i]);
    }
    arrayStr += "}";
    
    auto result = txn.exec_params(query.str(), arrayStr);
    
    std::vector<std::string> sections;
    sections.reserve(result.size());
    
    for ( const auto& row : result ) {
        sections.push_back(row[0].as<std::string>());
    }
    
    return sections;
}

namespace handlers {

using handler_t = messages::ResultMessage (*)(pqxx::connection& conn, const messages::TaskMessage&);

inline size_t countWordsInText(const std::string& text)
{
    if ( text.empty() ) { return 0; }
    
    size_t wordCount = 0;
    bool inWord = false;
    
    for ( char c : text ) {
        if ( std::isspace(c) || std::iscntrl(c) ) {
            if ( inWord ) {
                ++wordCount;
                inWord = false;
            }
        } else {
            inWord = true;
        }
    }
    
    if ( inWord ) { ++wordCount; }
    
    return wordCount;
}

inline messages::ResultMessage countWords(pqxx::connection& conn, const messages::TaskMessage& task)
{
    size_t totalWords = 0;
    auto sects = getAllSections(conn, task.sectionIds);
    
    for ( const auto& sect : sects ) {
        totalWords += countWordsInText(sect);
    }
    
    messages::ResultMessage result;
    result.type = messages::Type::WordsCount;
    result.result = totalWords;
    result.sectionsCount = static_cast<int>(task.sectionIds.size());
    
    return result;
}

inline std::vector<std::string> extractWords(const std::string& text)
{
    std::vector<std::string> words;
    std::string currentWord;
    
    for ( char c : text ) {
        if ( std::isalnum(c) || c == '\'' || c == '-' ) {
            currentWord += std::tolower(c);
        } else {
            if ( !currentWord.empty() ) {
                words.push_back(currentWord);
                currentWord.clear();
            }
        }
    }
    
    if ( !currentWord.empty() ) {
        words.push_back(currentWord);
    }
    
    return words;
}

inline messages::ResultMessage topN(pqxx::connection& conn, const messages::TaskMessage& task)
{
    if ( !task.n || *task.n <= 0 ) {
        messages::ResultMessage result;
        result.type = messages::Type::TopN;
        result.result = std::vector<std::pair<size_t, std::string>>{};
        result.sectionsCount = static_cast<int>(task.sectionIds.size());
        return result;
    }
    
    auto sects = getAllSections(conn, task.sectionIds);
    
    std::unordered_map<std::string, size_t> wordCounts;
    for ( const auto& sect : sects ) {
        auto words = extractWords(sect);
        for ( const auto& word : words ) {
            if ( !word.empty() ) {
                wordCounts[word]++;
            }
        }
    }
    
    std::vector<std::pair<size_t, std::string>> wordFreq;
    wordFreq.reserve(wordCounts.size());
    
    for ( const auto& [word, count] : wordCounts ) {
        wordFreq.emplace_back(count, word);
    }
    
    std::sort(wordFreq.begin(), wordFreq.end(), 
        [](const auto& a, const auto& b) {
            if ( a.first != b.first ) {
                return a.first > b.first;
            }
            return a.second < b.second;
        });
    
    size_t n = static_cast<size_t>(*task.n);
    if ( wordFreq.size() > n ) {
        wordFreq.resize(n);
    }
    
    messages::ResultMessage result;
    result.type = messages::Type::TopN;
    result.result = wordFreq;
    result.sectionsCount = static_cast<int>(task.sectionIds.size());
    
    return result;
}

inline std::vector<std::pair<size_t, std::string>> splitSentences(const std::string& text)
{
    std::vector<std::pair<size_t, std::string>> sentences;
    if ( text.empty() ) {
        return sentences;
    }
    
    std::string current;
    for ( size_t i = 0; i < text.length(); ++i ) {
        char c = text[i];
        current += c;
        
        if ( c == '.' || c == '!' || c == '?' ) {
            if ( i + 1 >= text.length() || std::isspace(text[i + 1]) || 
                 (i + 2 < text.length() && std::isupper(text[i + 2])) ) {
                std::string trimmed = current;
                size_t first = trimmed.find_first_not_of(" \t\n\r");
                if ( first != std::string::npos ) {
                    trimmed = trimmed.substr(first);
                    size_t last = trimmed.find_last_not_of(" \t\n\r");
                    if ( last != std::string::npos ) {
                        trimmed = trimmed.substr(0, last + 1);
                    }
                    if ( !trimmed.empty() ) {
                        sentences.emplace_back(std::pair{trimmed.size(), std::move(trimmed)});
                    }
                }
                current.clear();
            }
        }
    }
    
    if ( !current.empty() ) {
        std::string trimmed = current;
        size_t first = trimmed.find_first_not_of(" \t\n\r");
        if ( first != std::string::npos ) {
            trimmed = trimmed.substr(first);
            size_t last = trimmed.find_last_not_of(" \t\n\r");
            if ( last != std::string::npos ) {
                trimmed = trimmed.substr(0, last + 1);
            }
            if ( !trimmed.empty() ) {
                sentences.push_back(std::pair{trimmed.size(), std::move(trimmed)});
            }
        }
    }
    
    return sentences;
}

inline messages::ResultMessage tonality(pqxx::connection& conn, const messages::TaskMessage& task)
{
    static const std::unordered_set<std::string> positiveWords = {
        "good", "great", "excellent", "wonderful", "amazing", "fantastic", "beautiful",
        "happy", "joy", "love", "like", "best", "better", "perfect", "brilliant",
        "positive", "success", "win", "victory", "hope", "bright", "cheerful",
        "delight", "pleasure", "enjoy", "satisfaction", "pleased", "glad", "nice"
    };
    
    static const std::unordered_set<std::string> negativeWords = {
        "bad", "terrible", "awful", "horrible", "worst", "hate", "dislike",
        "sad", "angry", "fear", "worry", "problem", "difficult", "hard",
        "negative", "failure", "lose", "defeat", "despair", "dark", "gloomy",
        "pain", "suffering", "disappointment", "disgust", "horror", "evil", "wrong"
    };
    
    auto sects = getAllSections(conn, task.sectionIds);
    
    int positiveCount = 0;
    int negativeCount = 0;
    
    for ( const auto& sect : sects ) {
        auto words = extractWords(sect);
        for ( const auto& word : words ) {
            if ( positiveWords.find(word) != positiveWords.end() ) {
                positiveCount++;
            } else if ( negativeWords.find(word) != negativeWords.end() ) {
                negativeCount++;
            }
        }
    }
    
    std::string tonalityResult;
    if ( positiveCount > negativeCount * 1.2 ) {
        tonalityResult = "positive";
    } else if ( negativeCount > positiveCount * 1.2 ) {
        tonalityResult = "negative";
    } else {
        tonalityResult = "neutral";
    }
    
    std::ostringstream resultStr;
    resultStr << tonalityResult << " (positive: " << positiveCount 
              << ", negative: " << negativeCount << ")";
    
    messages::ResultMessage result;
    result.type = messages::Type::Tonality;
    result.result = resultStr.str();
    result.sectionsCount = static_cast<int>(task.sectionIds.size());
    
    return result;
}

inline messages::ResultMessage sortSentences(pqxx::connection& conn, const messages::TaskMessage& task)
{
    auto sects = getAllSections(conn, task.sectionIds);
    
    std::vector<std::pair<size_t, std::string>> allSentences;
    for ( const auto& sect : sects ) {
        auto sentences = splitSentences(sect);
        allSentences.insert(allSentences.end(), sentences.begin(), sentences.end());
    }
    
    std::sort(allSentences.begin(), allSentences.end(),
        [](const auto& a, const auto& b) {
            return a.first > b.first;
        });
    
    
    messages::ResultMessage result;
    result.type = messages::Type::SortSentences;
    result.result = std::move(allSentences);
    result.sectionsCount = static_cast<int>(task.sectionIds.size());
    
    return result;
}

static inline const std::unordered_map<messages::Type, handler_t> handlers = {
    { messages::Type::WordsCount, countWords },
    { messages::Type::TopN, topN },
    { messages::Type::Tonality, tonality },
    { messages::Type::SortSentences, sortSentences },
};

    
}
