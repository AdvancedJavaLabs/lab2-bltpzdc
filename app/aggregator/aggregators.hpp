#pragma once

#include "messages.hpp"

namespace aggregators {

using aggregator_t = void (*)(std::vector<messages::ResultMessage>&, messages::ResultMessage&);

static void countWordsAggregator(std::vector<messages::ResultMessage>& results, messages::ResultMessage& total)
{
    for ( const auto& res : results ) {
        total.wordsCount += res.wordsCount;
    }
}

static void topNAggregator(std::vector<messages::ResultMessage>& results, messages::ResultMessage& total)
{
    if ( results.empty() ) { return; }

    constexpr size_t top_n = 1000;

    std::unordered_map<std::string, size_t> wordCounts;
    for ( auto& r : results ) {
        auto topWords = std::move(r.topWords);
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

    if ( wordFreq.size() > top_n ) {
        wordFreq.resize(top_n);
    }
        
    total.topWords = std::move(wordFreq);
}

inline void sortSentencesAggregator(std::vector<messages::ResultMessage>& results, messages::ResultMessage& total)
{
    std::vector<std::pair<size_t, std::string>> allSentences;
    for ( auto& r : results ) {
        auto sentences = std::move(r.sortedSentences);
        allSentences.insert(allSentences.end(), sentences.begin(), sentences.end());
    }

    std::sort(allSentences.begin(), allSentences.end(),
        [](const auto& a, const auto& b) {
            return a.first > b.first;
        });

    total.sortedSentences = std::move(allSentences);
}

inline void tonalityAggregator(std::vector<messages::ResultMessage>& results, messages::ResultMessage& total)
{
    for ( const auto& res : results ) {
        total.tonality += res.tonality;
    }

    if ( total.tonality < 0 ) { total.tonality = -1; }
    else if ( total.tonality > 0 ) { total.tonality = 1; }
}

inline void replaceAggregator(std::vector<messages::ResultMessage>& results, messages::ResultMessage& total)
{
    std::string output;
    output.reserve(results[0].totalSections * 1024);
    for ( auto& res : results ) {
        output += std::move(res.replacedText);
    }

    total.replacedText = std::move(output);
}

inline const std::vector<aggregator_t> aggregators = {
    countWordsAggregator, topNAggregator, tonalityAggregator, sortSentencesAggregator, replaceAggregator,
};

}
