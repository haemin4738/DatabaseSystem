#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>
#include <vector>
#include <algorithm>
#include <string>
#include <cctype>
#include <queue>

#define NUM_THREADS 4

using namespace std;

typedef std::priority_queue<std::pair<string, int>, std::vector<std::pair<string, int>>, std::greater<std::pair<string, int>>> STRING_PQ;

string ToString(int val) {
    stringstream stream;
    stream << val;
    return stream.str();
}

streampos getDistPos(std::ifstream& inputFile, int threadIndex) {
	inputFile.seekg(0, std::ios::end);
	streampos endPos = inputFile.tellg();
	streampos distPos = (endPos / NUM_THREADS) * threadIndex;

    if(distPos == 0) return 0;

    inputFile.seekg(distPos);
    string line;
    getline(inputFile, line);
    distPos = inputFile.tellg();

	inputFile.seekg(0, std::ios::beg);
	return distPos;
}

void mergeFiles(int counter) {

    STRING_PQ minHeap;
    ifstream* runFiles = new ifstream[counter];

    for (int i = 1; i <= counter; i++) {
        string sortedInputFileName = "run" + ToString(i) + ".txt";
        runFiles[i - 1].open(sortedInputFileName.c_str());
        string firstValue;
        runFiles[i - 1] >> firstValue;
        minHeap.push(pair<string, int>(firstValue, i - 1));
    }

    string outputFileName = "merge&sort.txt";
    ofstream outputFile(outputFileName.c_str(), ios::trunc);

    string lastWordName = "";
    int lastWordCount = 1;
    while (minHeap.size() > 0) {
        pair<string, int> minPair = minHeap.top();
        minHeap.pop();

        if (lastWordName == minPair.first) {
            lastWordCount += 1;
       	}
        else {
        	if (lastWordName != "") {
        		outputFile << lastWordName << " " << "[1";
                for (int k{ 1 }; k < lastWordCount; ++k) {
                    outputFile << ",1";
       			}
                outputFile << "]" << endl;
       		}
            lastWordName = minPair.first;
        	lastWordCount = 1;
       	}

        string nextValue;
        flush(outputFile);
        if (runFiles[minPair.second] >> nextValue) {
            minHeap.push(pair<string, int>(nextValue, minPair.second));
        }
    }

	if(lastWordName != "") {
		outputFile << lastWordName << " " << "[1";
		for (int k{1}; k < lastWordCount; ++k) 
			outputFile << ",1";
		outputFile << "]" << endl;
	}
    for (int i = 1; i <= counter; i++) {
        runFiles[i - 1].close();
    }
    outputFile.close();
    delete[] runFiles;

}

void makeRuns(vector<string>& values, int size, int numberOfChunks) {
	string outputFileName = "run" + ToString(numberOfChunks) + ".txt";
	ofstream outputFile(outputFileName.c_str(), ios::trunc | ios::out);
	sort(values.begin(), values.end()); 

    for (int i = 0; i < size; i++) {
		outputFile << values[i] << '\n';
	}
	outputFile.close();
}

void reduceAndWrite() {

    ifstream inputFile("merge&sort.txt");
    ofstream outputFile("reduce.txt", ios::trunc | ios::out);
    string line;

    while (getline(inputFile, line)) {
        if(line.empty()) continue;
		stringstream ss(line);
		string word;
		string countArrayString;
		ss >> word >> countArrayString;

        int count = countArrayString.size();
        count -= 2;
       	count /= 2;
        ++count;

        outputFile << word << " " << count << std::endl;
	}
    
	inputFile.close();
	outputFile.close();
}

vector<string> split(std::string& input) {
    vector<std::string> ret;
    stringstream ss(input);
    string temp;

    while (ss >> temp) {
        string processedWord = "";
        if (isalnum(static_cast<unsigned char>(temp[0])))
            processedWord += temp[0];

        for (int i{ 1 }; i < temp.size(); ++i) {
            if(temp[i] > 127)
                continue;
            if (isalpha(static_cast<unsigned char>(temp[i])))
                processedWord += tolower(static_cast<unsigned char>(temp[i]));
            else if (isdigit(static_cast<unsigned char>(temp[i])))
                processedWord += temp[i];
            else {
                if(!processedWord.empty())
                    processedWord += temp[i];
            }
        }

        if (processedWord.size() >= 1)
            if (!isalnum(static_cast<unsigned char>(processedWord[processedWord.size() - 1])))
                processedWord.erase(processedWord.end() - 1);

        if (!processedWord.empty())
            ret.push_back(processedWord);
    }

    return ret;
}

int main(int argc, char **argv) {
	if(argc != 2) {
		cout << "USAGE : ./main <file>" << endl;
		exit(1);
	}
	string inputFileName = argv[1];


    auto start = chrono::high_resolution_clock::now();
    int numberOfChunks = 1;
    int chunkSize = 8 * 1024 * 1024;
    ifstream inputFile(inputFileName);
	if(!inputFile) {
		cout << "Failed to open input file" << endl;
		exit(1);
	}

   	vector<streampos> distStartPositions(NUM_THREADS);
	vector<streampos> distEndPositions(NUM_THREADS);

   	for (int i = 0; i < NUM_THREADS; i++) {
		distStartPositions[i] = getDistPos(inputFile, i);
	}

	for (int i = 0; i < NUM_THREADS - 1; i++) {
		distEndPositions[i] = distStartPositions[i + 1];
	}

	inputFile.seekg(0, ios::end);
	distEndPositions[NUM_THREADS - 1] = inputFile.tellg();
 
    inputFile.close();
    vector<thread> threads;
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread([&, i]() {
            ifstream distInputFile(inputFileName);
			distInputFile.seekg(distStartPositions[i]);
			string line;
			vector<string> inputValues;
			int currentCount = 0;
			bool unprocessedData = true;
            while (getline(distInputFile, line)) {
                if(distInputFile.tellg() >= distEndPositions[i])
					break;

                unprocessedData = true;
				vector<string> words = split(line);
                for (int i = 0; i < words.size(); i++) {
					currentCount++;
					inputValues.push_back(words[i]);
                    if (currentCount == chunkSize) {
						makeRuns(inputValues, currentCount, numberOfChunks);
						numberOfChunks++;
						currentCount = 0;
						unprocessedData = false;
						inputValues.clear();
					}
				}
			}
            if (unprocessedData) {
				makeRuns(inputValues, currentCount, numberOfChunks);
                inputValues.clear();
			}
            else {
				numberOfChunks--;
			}
            distInputFile.close();
		}));
	}

    for (int i = 0; i < NUM_THREADS; i++) {
		threads[i].join();
	}


    if (numberOfChunks != 0) {
        mergeFiles(numberOfChunks);
        reduceAndWrite();
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end - start;
    cout << "Time taken: " << diff.count() << " s" << endl;
    return 0;
}
