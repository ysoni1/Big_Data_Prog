import findspark
from pyspark import *
import string
import enchant
import sys
import random
import googlesearch

findspark.init()

conf = SparkConf().setMaster("local").setAppName("HW2")
sc = SparkContext(conf=conf)

albets = 'abcdefghijklmnopqrstuvwxyz'
dictionary = enchant.Dict('en-US')


def cmnLtr(charArray, index=0):  # helps find the most used letter
    try:
        char = charArray[index][
            0]
        if str(
                char
        ) not in albets:
            return cmnLtr(charArray, index + 1)
        return char
    except Exception as e:
        print(e)
        sys.exit(0)
    return None


def mvTxt(text, howMuch):  # helps indent or add spaces as needed
    newString = ""
    for char in text:
        if char.lower(
        ) not in albets:  
            newString += char
        else:
            newIndex = string.ascii_lowercase.index(char.lower()) + howMuch
            if newIndex > 25:  
                newIndex -= 26
            if newIndex < 0:  
                newIndex += 26
            newString += albets[newIndex]
    return newString


def diffAlphs(letter, compareTo='e'):  # difference between alphabets
    return string.ascii_lowercase.index(
        compareTo.lower()) - string.ascii_lowercase.index(letter.lower())


def chckWrds(decrypted_words):  # scans the words and identifies valid or invalid
    passed = 0
    failed = 0
    test_count = int(len(decrypted_words) / 20)
    for _ in range(test_count):  # check 5% of words
        index = random.randint(0, len(decrypted_words) - 1)
        word = ''.join(c for c in decrypted_words[index]
                       if str(c).isalnum()).lower()
        if not word:
            test_count -= 1
            continue
        if dictionary.check(word):
            passed += 1
        else:
            failed += 1
    if failed == 0:
        print()
        print('Tested {} word(s): 100% validity.'.format(test_count))
        return True
    passed_percentage = (passed / test_count) * 100
    print()
    print('Tested {} word(s): {}% validity.'.format(
        test_count, "{0:.2f}".format(passed_percentage)))
    if passed_percentage >= 75:
        return True
    return False


def check_google(text):  # takes the first 20 words and searchs as a query on google
    query_length = min(20, len(text))
    query_text = ""
    for i in range(query_length):
        query_text += text[
            i] + " "  
    search_results = googlesearch.search(query=query_text,
                                         tld="com",
                                         lang='en',
                                         num=3,
                                         stop=3,
                                         pause=2.0)
    if search_results:
        print("Try these sources:")
        for result in search_results:
            print(result)
            print()


print()
filename = input(
    "Name of the text file: ")
auto_check_complete = False

print()
res = input(
    "(Yes) = Automatic decode (No) = Manual Decode: "
)

if res.lower().startswith('y'):
    auto_check_complete = True

textFile = sc.textFile('{}.txt'.format(filename)).cache()  # extension of the file pressumed
chars = textFile.flatMap(lambda line: list(line))
charMap = chars.map(lambda char: (char.lower(), 1)).reduceByKey(
    lambda k, v: k + v).collect()
charMap.sort(key=lambda charTuple: charTuple[1], reverse=True)
letter = cmnLtr(charMap)

letterMarkers = ['e', 't', 'a', 'o', 'i', 'n', 's', 'h', 'r',
                 'd']
letterMarkerIndex = 0
validWords = False
print("Most used letter: {}".format(letter))


while not validWords:
    if letterMarkerIndex > len(letterMarkers) - 1:
        print("Exhausted the 10 most common letters. Quitting...")
        sys.exit(0)
    letterMarker = letterMarkers[letterMarkerIndex]
    diff = diffAlphs(letter, letterMarker)
    print()
    print("Adjusting for '{}'. Shifting text by {}...".format(
        letterMarker, diff))
    print()
    adjustedText = mvTxt(chars.collect(), diff)
    print(adjustedText)
    if auto_check_complete:
        adjustedWords = adjustedText.split()
        if chckWrds(adjustedWords):
            validWords = True

            print()
            print(
                "Deciphering complete. Most of the words are dictionary words."
            )
    else:
        res = input("Does the above seem correct? (y/n): ")
        if res.lower().startswith('y'):
            validWords = True
    letterMarkerIndex += 1


with open("decryptedFile_{}.txt".format(filename),
          'w+') as f:  # save decrypted text to a file
    f.writelines(adjustedText)
    f.close()
print()
print("File decrypted and saved as : decryptedFile_{}.txt".format(filename))
print()
check_google(adjustedWords)