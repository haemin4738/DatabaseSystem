#include <stdio.h>

// 각 단어와 해당 단어의 카운트를 저장할 구조체
struct WordCount
{
    char word[100];
    int count;
};

int main()
{
    FILE *inputFile1, *outputFile;
    char word[100];

    // 열고 읽을 첫 번째 입력 파일
    inputFile1 = fopen("input1.txt", "r");
    if (inputFile1 == NULL)
    {
        perror("Error opening input1.txt");
        return 1;
    }

    // 결과를 저장할 출력 파일
    outputFile = fopen("result.txt", "w");
    if (outputFile == NULL)
    {
        perror("Error creating result.txt");
        return 1;
    }

    // 단어 카운트를 저장할 배열
    struct WordCount wordCountArray[1000];
    int wordCount = 0;

    // 첫 번째 입력 파일을 읽고 단어 카운트 업데이트
    while (fscanf(inputFile1, "%s", word) != EOF)
    {
        int found = 0;
        for (int i = 0; i < wordCount; i++)
        {
            if (strcmp(word, wordCountArray[i].word) == 0)
            {
                wordCountArray[i].count++;
                found = 1;
                break;
            }
        }
        if (!found)
        {
            strcpy(wordCountArray[wordCount].word, word);
            wordCountArray[wordCount].count = 1;
            wordCount++;
        }
    }

    // 파일 닫기
    fclose(inputFile1);

    // 결과를 파일에 저장
    for (int i = 0; i < wordCount; i++)
    {
        fprintf(outputFile, "%s: %d\n", wordCountArray[i].word, wordCountArray[i].count);
    }

    // 출력 파일 닫기
    fclose(outputFile);

    printf("Results saved in result.txt.\n");

    return 0;
}