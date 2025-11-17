#pragma once

#include "messages.hpp"

namespace aggregators {

using aggregator_t = messages::ResultMessage (*)(std::vector<messages::ResultMessage>&);

static messages::ResultMessage countWordsAggregator(std::vector<messages::ResultMessage>& results)
{
    messages::ResultMessage aggregated;
    
    size_t total = 0;
    for ( const auto& r : results ) {
        total += std::get<size_t>(r.result);
    }
    aggregated.result = total;

    return aggregated;
}

static messages::ResultMessage topNAggregator(std::vector<messages::ResultMessage>& results)
{
    messages::ResultMessage aggregated;

    std::unordered_map<std::string, size_t> wordCounts;
    for ( const auto& r : results ) {
        const auto& topWords = std::get<std::vector<std::pair<size_t, std::string>>>(r.result);
        for ( auto& [count, word] : topWords ) {
            wordCounts[std::move(word)] += count;
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

    if ( !results.empty() && results[0].n && *results[0].n > 0 ) {
        size_t n = static_cast<size_t>(*results[0].n);
        if ( wordFreq.size() > n ) {
            wordFreq.resize(n);
        }
        aggregated.n = results[0].n;
    }
        
    aggregated.result = wordFreq;
    return aggregated;
}

messages::ResultMessage sortSentencesAggregator(std::vector<messages::ResultMessage>& results)
{
    messages::ResultMessage aggregated;

    std::vector<std::pair<size_t, std::string>> allSentences;
    for ( const auto& r : results ) {
        const auto& sentences = std::get<std::vector<std::pair<size_t, std::string>>>(r.result);
        allSentences.insert(allSentences.end(), sentences.begin(), sentences.end());
    }

    std::sort(allSentences.begin(), allSentences.end(),
        [](const auto& a, const auto& b) {
            return a.first > b.first;
        });

    aggregated.result = allSentences;
    return aggregated;
}

messages::ResultMessage tonalityAggregator(std::vector<messages::ResultMessage>& results)
{
    messages::ResultMessage aggregated;
    
    int totalPositive = 0;
    int totalNegative = 0;
    
    for ( const auto& r : results ) {
        const std::string& tonalityStr = std::get<std::string>(r.result);
        
        size_t posPos = tonalityStr.find("positive: ");
        size_t negPos = tonalityStr.find("negative: ");
        
        if ( posPos != std::string::npos ) {
            size_t numStart = posPos + 10;
            size_t numEnd = tonalityStr.find_first_not_of("0123456789", numStart);
            if ( numEnd == std::string::npos ) numEnd = tonalityStr.length();
            totalPositive += std::stoi(tonalityStr.substr(numStart, numEnd - numStart));
        }
        
        if ( negPos != std::string::npos ) {
            size_t numStart = negPos + 10;
            size_t numEnd = tonalityStr.find_first_not_of("0123456789", numStart);
            if ( numEnd == std::string::npos ) numEnd = tonalityStr.length();
            totalNegative += std::stoi(tonalityStr.substr(numStart, numEnd - numStart));
        }
    }
    
    std::string tonalityResult;
    if ( totalPositive > totalNegative * 1.2 ) {
        tonalityResult = "positive";
    } else if ( totalNegative > totalPositive * 1.2 ) {
        tonalityResult = "negative";
    } else {
        tonalityResult = "neutral";
    }
    
    std::ostringstream resultStr;
    resultStr << tonalityResult << " (positive: " << totalPositive 
              << ", negative: " << totalNegative << ")";
    
    aggregated.result = resultStr.str();
    return aggregated;
}

inline const std::unordered_map<messages::Type, aggregator_t> aggregators = {
    { messages::Type::WordsCount, countWordsAggregator },
    { messages::Type::TopN, topNAggregator },
    { messages::Type::Tonality, tonalityAggregator },
    { messages::Type::SortSentences, sortSentencesAggregator },
};

}
