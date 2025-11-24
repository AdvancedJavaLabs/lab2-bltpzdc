#pragma once

#include "messages.hpp"
#include <pqxx/pqxx>

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

inline std::vector<std::string> getAllSections(pqxx::connection& conn, const std::vector<int>& sectionIds)
{
    if ( sectionIds.empty() ) { return {}; }
    
    pqxx::work txn(conn);
    
    std::ostringstream query;
    query << "SELECT content FROM sections WHERE id = ANY($1::int[]) ORDER BY id";
    
    std::string arrayStr = "{";
    for ( size_t i = 0; i < sectionIds.size(); ++i ) {
        if ( i > 0 ) { arrayStr += ","; }
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

using handler_t = void (*)(const std::vector<std::string>&, messages::ResultMessage&);

inline size_t countWordsInText(const std::string& text)
{
    if ( text.empty() ) { return 0; }
    
    size_t wordCount = 0;
    bool inWord = false;
    
    for ( char c : text ) {
        if ( std::isspace(c) or std::iscntrl(c) ) {
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

inline void countWords(const std::vector<std::string>& sections, messages::ResultMessage& result)
{
    size_t totalWords = 0;    
    for ( const auto& sect : sections ) {
        totalWords += countWordsInText(sect);
    }
    
    result.wordsCount = totalWords;
}

inline std::vector<std::string> extractWords(const std::string& text)
{
    std::vector<std::string> words;
    std::string currentWord;
    
    for ( char c : text ) {
        if ( std::isalnum(c) or c == '\'' or c == '-' ) {
            currentWord += std::tolower(c);
        } else {
            if ( not currentWord.empty() ) {
                words.push_back(currentWord);
                currentWord.clear();
            }
        }
    }
    
    if ( not currentWord.empty() ) { words.push_back(currentWord); }
    
    return words;
}

inline void topN(const std::vector<std::string>& sections, messages::ResultMessage& result)
{   
    std::unordered_map<std::string, size_t> wordCounts;
    for ( const auto& sect : sections ) {
        auto words = extractWords(sect);
        for ( const auto& word : words ) {
            if ( not word.empty() ) {
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
    
    constexpr size_t n = 1000;
    if ( wordFreq.size() > n ) {
        wordFreq.resize(n);
    }
    
    result.topWords = std::move(wordFreq);
}

inline std::vector<std::pair<size_t, std::string>> splitSentences(const std::string& text)
{
    if ( text.empty() ) { return {}; }
    
    std::vector<std::pair<size_t, std::string>> sentences;
    std::string current;
    for ( size_t i = 0; i < text.length(); ++i ) {
        char c = text[i];
        current += c;
        
        if ( c == '.' or c == '!' or c == '?' ) {
            if ( i + 1 >= text.length() or std::isspace(text[i + 1]) or 
                 (i + 2 < text.length() and std::isupper(text[i + 2])) ) {
                std::string trimmed = current;
                size_t first = trimmed.find_first_not_of(" \t\n\r");
                if ( first != std::string::npos ) {
                    trimmed = trimmed.substr(first);
                    size_t last = trimmed.find_last_not_of(" \t\n\r");
                    if ( last != std::string::npos ) {
                        trimmed = trimmed.substr(0, last + 1);
                    }
                    if ( not trimmed.empty() ) {
                        sentences.emplace_back(std::pair{trimmed.size(), std::move(trimmed)});
                    }
                }
                current.clear();
            }
        }
    }
    
    if ( not current.empty() ) {
        std::string trimmed = current;
        size_t first = trimmed.find_first_not_of(" \t\n\r");
        if ( first != std::string::npos ) {
            trimmed = trimmed.substr(first);
            size_t last = trimmed.find_last_not_of(" \t\n\r");
            if ( last != std::string::npos ) {
                trimmed = trimmed.substr(0, last + 1);
            }
            if ( not trimmed.empty() ) {
                sentences.push_back(std::pair{trimmed.size(), std::move(trimmed)});
            }
        }
    }
    
    return sentences;
}

inline void tonality(const std::vector<std::string>& sections, messages::ResultMessage& result)
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
    
    int positiveCount = 0;
    int negativeCount = 0;
    
    for ( const auto& sect : sections ) {
        auto words = extractWords(sect);
        for ( const auto& word : words ) {
            if ( positiveWords.find(word) != positiveWords.end() ) {
                positiveCount++;
            } else if ( negativeWords.find(word) != negativeWords.end() ) {
                negativeCount++;
            }
        }
    }
    
    int tonalityResult;
    if ( positiveCount > negativeCount * 1.2 ) {
        tonalityResult = 1;
    } else if ( negativeCount > positiveCount * 1.2 ) {
        tonalityResult = -1;
    } else {
        tonalityResult = 0;
    }
    
    result.tonality = tonalityResult;
}

inline void sortSentences(const std::vector<std::string>& sections, messages::ResultMessage& result)
{
    std::vector<std::pair<size_t, std::string>> allSentences;
    for ( const auto& sect : sections ) {
        auto sentences = splitSentences(sect);
        allSentences.insert(allSentences.end(), sentences.begin(), sentences.end());
    }
    
    std::sort(allSentences.begin(), allSentences.end(),
        [](const auto& a, const auto& b) {
            return a.first > b.first;
        });
    
    
    result.sortedSentences = std::move(allSentences);
}

inline void replaceWords(const std::vector<std::string>& sections, messages::ResultMessage& result)
{
    std::string output;
    output.reserve( sections.size() * 1024 );

    const std::string from = "Natasha";
    const std::string to   = "Rzhevsky";

    for (const auto& sec : sections)
    {
        std::string tmp;
        tmp.reserve(sec.size());

        size_t start = 0;
        while (true)
        {
            size_t pos = sec.find(from, start);
            if (pos == std::string::npos) {
                tmp.append(sec, start, std::string::npos);
                break;
            }

            tmp.append(sec, start, pos - start);
            tmp += to;
            start = pos + from.size();
        }

        output += tmp;
    }

    result.replacedText = std::move(output);
}


static inline const std::vector<handler_t> handlers = {
    countWords, topN, tonality, sortSentences, replaceWords,
};
    
}
