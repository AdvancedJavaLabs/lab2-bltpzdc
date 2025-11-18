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
    if ( results.empty() ) { return {}; }

    messages::ResultMessage aggregated;
    aggregated.n = results[0].n;

    std::unordered_map<std::string, size_t> wordCounts;
    for ( auto& r : results ) {
        auto topWords = std::get<std::vector<std::pair<size_t, std::string>>>(std::move(r.result));
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

    if ( aggregated.n ) {
        auto n = static_cast<size_t>(*aggregated.n);
        if ( wordFreq.size() > n ) {
            wordFreq.resize(n);
        }
    }
        
    aggregated.result = wordFreq;
    return aggregated;
}

messages::ResultMessage sortSentencesAggregator(std::vector<messages::ResultMessage>& results)
{
    messages::ResultMessage aggregated;

    std::vector<std::pair<size_t, std::string>> allSentences;
    for ( auto& r : results ) {
        auto sentences = std::get<std::vector<std::pair<size_t, std::string>>>(std::move(r.result));
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
    
    for ( auto& r : results ) {
        auto tonalityStr = std::get<std::string>(std::move(r.result));
        
        auto posPos = tonalityStr.find("positive: ");
        auto negPos = tonalityStr.find("negative: ");
        
        if ( posPos != std::string::npos ) {
            auto numStart = posPos + 10;
            auto numEnd = tonalityStr.find_first_not_of("0123456789", numStart);
            if ( numEnd == std::string::npos ) { numEnd = tonalityStr.length(); }
            totalPositive += std::stoi(tonalityStr.substr(numStart, numEnd - numStart));
        }
        
        if ( negPos != std::string::npos ) {
            auto numStart = negPos + 10;
            auto numEnd = tonalityStr.find_first_not_of("0123456789", numStart);
            if ( numEnd == std::string::npos ) { numEnd = tonalityStr.length(); }
            totalNegative += std::stoi(tonalityStr.substr(numStart, numEnd - numStart));
        }
    }
    
    std::string tonalityResult;
    if ( totalPositive > totalNegative * 1.2 ) { tonalityResult = "positive"; }
    else if ( totalNegative > totalPositive * 1.2 ) { tonalityResult = "negative"; }
    else { tonalityResult = "neutral"; }
    
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
