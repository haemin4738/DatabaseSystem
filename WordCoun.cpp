#include <iostream>
#include <fstream>
#include <random>
#include <map>
#include <sstream>
#include <thread>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <atomic>
#include <mutex>
#include <condition_variable>

// 무작위 텍스트를 생성하는 함수
std::string generateRandomText(int num_lines, int words_per_line) {
    std::string text;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> word_length(3, 10);
    std::uniform_int_distribution<> word_char(97, 122); // 소문자 알파벳 ASCII 값

    for (int i = 0; i < num_lines; ++i) {
        for (int j = 0; j < words_per_line; ++j) {
            int length = word_length(gen);
            std::string word;
            for (int k = 0; k < length; ++k) {
                word += static_cast<char>(word_char(gen));
            }
            text += word + " ";
        }
        text += "\n";
    }

    return text;
}

// Mapper 함수: key-value 쌍을 생성함
void mapper(const std::string& line, std::map<std::string, int>& intermediate) {
    std::istringstream iss(line);
    std::string word;

    while (iss >> word) {
        // 필요시 구두점 제거
        std::string cleaned_word;
        for (char c : word) {
            if (isalnum(c)) {
                cleaned_word += tolower(c);
            }
        }

        if (!cleaned_word.empty()) {
            intermediate[cleaned_word]++;
        }
    }
}

// 병렬 처리를 위한 함수
void parallelWordCount(std::vector<std::string>& lines, std::atomic<int>& lines_processed,
    std::map<std::string, int>& word_counts, std::mutex& mtx, std::condition_variable& cv,
    int num_threads) {
    std::vector<std::thread> threads;
    std::atomic<int> current_line(0);

    auto mapperFunc = [&]() {
        std::map<std::string, int> intermediate;
        while (true) {
            int line_idx = current_line.fetch_add(1);
            if (line_idx >= lines.size()) {
                break;
            }
            mapper(lines[line_idx], intermediate);
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& pair : intermediate) {
            word_counts[pair.first] += pair.second;
        }

        lines_processed.fetch_add(1);
        cv.notify_one();
        };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(mapperFunc);
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

int main() {
    // 무작위 텍스트가 담길 input.txt 파일 생성
    std::ofstream outputFile("input.txt");

    if (outputFile.is_open()) {
        std::string random_text = generateRandomText(5, 20); // 무작위 텍스트 생성 (5줄, 각 줄에 20개 단어)
        outputFile << random_text;

        outputFile.close();
        std::cout << "input.txt 파일 생성 완료." << std::endl;
    }
    else {
        std::cerr << "input.txt 파일을 생성할 수 없습니다." << std::endl;
        return 1;
    }

    // input.txt 파일 읽고 병렬 처리로 WordCount 수행
    std::ifstream inputFile("input.txt");
    std::string line;
    std::vector<std::string> lines;
    std::map<std::string, int> word_counts;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<int> lines_processed(0);
    int num_threads = std::thread::hardware_concurrency(); // 시스템의 스레드 개수 가져오기

    if (inputFile.is_open()) {
        while (std::getline(inputFile, line)) {
            lines.push_back(line);
        }

        inputFile.close();

        auto startTime = std::chrono::steady_clock::now();

        parallelWordCount(lines, lines_processed, word_counts, mtx, cv, num_threads);

        auto endTime = std::chrono::steady_clock::now();
        auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
        std::cout << "전체 처리 시간: " << totalTime << "초" << std::endl;
        std::cout << "각 단어 처리 시간:" << std::endl;

        // 단어 처리 시간 출력
        std::vector<int> processingTimes;
        for (const auto& pair : word_counts) {
            std::cout << pair.first << ": " << pair.second << std::endl;
            processingTimes.push_back(pair.second);
        }

        // 각 단어 처리 시간의 평균과 최소/최대값 출력
        double avgProcessingTime = std::accumulate(processingTimes.begin(), processingTimes.end(), 0.0) / processingTimes.size();
        int minProcessingTime = *std::min_element(processingTimes.begin(), processingTimes.end());
        int maxProcessingTime = *std::max_element(processingTimes.begin(), processingTimes.end());
        std::cout << "각 단어 평균 처리 시간: " << avgProcessingTime << "초" << std::endl;
        std::cout << "각 단어 최소 처리 시간: " << minProcessingTime << "초" << std::endl;
        std::cout << "각 단어 최대 처리 시간: " << maxProcessingTime << "초" << std::endl;
    }
    else {
        std::cerr << "input.txt 파일을 열 수 없습니다." << std::endl;
        return 1;
    }

    return 0;
}