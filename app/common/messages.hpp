#pragma once

#include "json.hpp"
#include <cstddef>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>
#include <unordered_map>
#include <string>

namespace messages {

struct TaskMessage {
    int taskId{0};
    std::vector<int> sectionIds{};
    int totalSections{0};
    long startTime{0};

    std::string toJson() const
    {
        nlohmann::json json;
        json["task_id"] = taskId;
        json["total_sections"] = totalSections;
        json["section_ids"] = sectionIds;
        json["start_time"] = startTime;

        return json.dump();
    } 
    
    static TaskMessage fromJson(const std::string& msg)
    {
        TaskMessage task;
        auto json = nlohmann::json::parse(msg);
        if ( json["task_id"].is_number() ) { task.taskId = json["task_id"]; }
        if ( json["total_sections"].is_number() ) { task.totalSections = json["total_sections"]; }
        if ( json["section_ids"].is_array() ) { task.sectionIds = json["section_ids"].get<decltype(task.sectionIds)>(); }
        if ( json["start_time"].is_number() ) { task.startTime = json["start_time"]; }
        
        return task;
    }
};

struct ResultMessage {
    int taskId{0};
    int sectionsCount{0};
    int totalSections{0};
    long startTime{0};

    size_t wordsCount{0};
    std::vector<std::pair<size_t, std::string>> topWords;
    std::vector<std::pair<size_t, std::string>> sortedSentences;
    int tonality{0};
    std::string replacedText;

    std::string toJson() const
    {
        nlohmann::json json;

        json["task_id"] = taskId;
        json["sections_count"] = sectionsCount;
        json["total_sections"] = totalSections;
        json["start_time"] = startTime;

        json["words_count"] = wordsCount;

        json["top_words"] = nlohmann::json::array();
        for (auto& p : topWords)
            json["top_words"].push_back({ {"count", p.first}, {"text", p.second} });

        json["sorted_sentences"] = nlohmann::json::array();
        for (auto& p : sortedSentences)
            json["sorted_sentences"].push_back({ {"count", p.first}, {"text", p.second} });

        json["tonality"] = tonality;
        json["replaced_text"] = replacedText;

        return json.dump();
    }

    static ResultMessage fromJson(const std::string_view msg)
    {
        auto json = nlohmann::json::parse(msg);

        ResultMessage r;

        r.taskId = json.value("task_id", 0);
        r.sectionsCount = json.value("sections_count", 0);
        r.totalSections = json.value("total_sections", 0);
        r.startTime = json.value("start_time", 0L);

        r.wordsCount = json.value("words_count", 0);

        if (json.contains("top_words"))
            for (auto& j : json["top_words"])
                r.topWords.emplace_back(
                    j.value("count",0),
                    j.value("text","")
                );

        if (json.contains("sorted_sentences"))
            for (auto& j : json["sorted_sentences"])
                r.sortedSentences.emplace_back(
                    j.value("count",0),
                    j.value("text","")
                );

        r.tonality = json.value("tonality", 0);
        r.replacedText = json.value("replaced_text", "");

        return r;
    }
};


}
