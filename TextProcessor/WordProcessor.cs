//Copyright (c) Microsoft Corporation 
//
//All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0   
//
//THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  
//
//See the Apache Version 2.0 License for specific language governing permissions and limitations under the License. 

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TextProcessor
{
    //the word processor builds a word and calls the stemmer
    public class WordProcessor
    {
        string[] specialhtml = { "Aacute", "aacute", "Acirc", "acirc", "acute", "AElig", "aelig", "Agrave", "agrave", "amp", "Aring", "aring", "Atilde", "atilde", "Auml", "auml", "bdquo", "brkbar", "brvbar", "Ccedil", "ccedil", "cedil", "cent", "clubs", "copy", "curren", "dagger", "Dagger", "darr", "deg", "diams", "divide", "Eacute", "eacute", "Ecirc", "ecirc", "Egrave", "egrave", "ETH", "eth", "Euml", "euml", "frac12", "frac14", "frac34", "frasl", "gt", "hearts", "Iacute", "iacute", "Icirc", "icirc", "iexcl", "Igrave", "igrave", "iquest", "Iuml", "iuml", "laquo", "larr", "ldquo", "lsaquo", "lsquo", "lt", "macr", "hibar", "mdash", "micro", "middot", "nbsp", "ndash", "not", "Ntilde", "ntilde", "Oacute", "oacute", "Ocirc", "ocirc", "Ograve", "ograve", "oline", "ordf", "ordm", "Oslash", "oslash", "Otilde", "otilde", "Ouml", "ouml", "para", "permil", "plusmn", "pound", "quot", "raquo", "rarr", "rdquo", "reg", "rsaquo", "rsquo", "sbquo", "sect", "shy", "spades", "sup1", "sup2", "sup3", "szlig", "THORN", "thorn", "times", "trade", "Uacute", "uacute", "uarr", "Ucirc", "ucirc", "Ugrave", "ugrave", "die", "uml", "Uuml", "uuml", "Yacute", "yacute", "yen", "yuml" };
        char[] specialreplacements = { 'A', 'a', 'A', 'a', 'Z', 'A', 'a', 'A', 'a', '|', 'A', 'a', 'A', 'a', 'A', 'a', '|', '|', '|', 'C', 'c', 'Z', '|', '|', '|', '|', '|', '|', '|', '|', '|', '|', 'E', 'd', 'E', 'd', 'E', 'e', 'E', 'e', 'E', 'e', '|', '|', '|', '|', '|', '|', 'I', 'i', 'I', 'i', '|', 'I', 'i', 'X', 'I', 'i', 'X', '|', 'X', 'X', 'X', '|', 'Z', 'Z', '|', '|', '|', 'W', '|', '|', 'N', 'n', 'O', 'o', 'O', 'o', 'O', 'o', '|', '|', '|', 'O', 'o', 'O', 'o', 'O', 'o', '|', '|', '|', '|', 'X', 'X', '|', 'X', '|', 'X', 'X', 'X', '|', 'W', '|', '|', '|', '|', '|', '|', '|', '|', '|', 'U', 'u', '|', 'U', 'u', 'U', 'u', 'Z', 'Z', 'U', 'u', 'Y', 'y', '|', 'y' };
        char[] prettypunctuation = { (char)34, (char)171, (char)187, (char)191, (char)8216, (char)8217, (char)8218, (char)8220, (char)8221, (char)8222, (char)8249, (char)8250, '(', ')' };

        Dictionary<string, char> specialhtml_dict;
        HashSet<char> prettypunctuation_dict;
        Stemmer porterstemmer;
        public char[] wordbuffer;
        int bufferlength;
        int wordpos;
        bool word_overflow;
        int[] hyphenations;
        bool[] isInteger;
        int hyphenationcounter;
        char[] tempbuffer;
        public WordProcessor(int size)
        {
            //initialize stemmer
            porterstemmer = new Stemmer();
            wordbuffer = new char[size];
            tempbuffer = new char[3 * size];
            bufferlength = size;
            specialhtml_dict = new Dictionary<string, char>();
            for (int i = 0; i < specialhtml.Length; i++)
            {
                specialhtml_dict.Add(specialhtml[i], specialreplacements[i]);
            }
            prettypunctuation_dict = new HashSet<char>();
            foreach (char letter in prettypunctuation)
            {
                prettypunctuation_dict.Add(letter);
            }
            hyphenations = new int[3];
            isInteger = new bool[4];
        }
        public void initialize()
        {
            wordpos = 0;
            word_overflow = false;
        }
        //returns true if letter was added to word
        //returns false if letter was a formating character
        // #TAB#, #N# and #R# count as formating characters as well
        public bool AddLetter(char letter)
        {
            //check whether we have an overflow
            if (word_overflow)
            {
                if ((letter == ' ') || (letter == '\n') || (letter == '\r') || (letter == '\t'))
                {
                    return false;
                }
                return true;
            }
            //check whether we encountered an HTML special character
            //if so, replace the letter
            if ((letter == ';') && (wordpos < bufferlength))
            {
                ReduceSpecialCharacter(ref letter);
            }
            //do we have a null character? interpret as empty space
            if (letter == (char)0)
            {
                return true;
            }
            //check if we have a quotation or other delimiting character at first position
            if ((prettypunctuation_dict.Contains(letter) || letter == 39) && (wordpos == 0 || wordpos == 1))
            {
                //interpret letter as space
                letter = ' ';
            }
            if (prettypunctuation_dict.Contains(letter) && wordpos > 0)
            {
                //interpret letter as simpler ignorable character
                letter = ';';
            }
            //check if the letter is a formatting character
            if ((letter == ' ') || (letter == '\n') || (letter == '\r') || (letter == '\t'))
            {
                if (wordpos > 0)
                {
                    //if previous letters were comma etc. delete this letter
                    for (int i = (wordpos - 1); i >= 0; i--)
                    {
                        char prevletter = wordbuffer[i];
                        if ((prevletter == ',') || (prevletter == '.') || (prevletter == ';') || (prevletter == ':') || (prevletter == '?') || (prevletter == '!') || (prevletter == 39))
                        {
                            wordpos = i;
                        }
                        else
                        {
                            break;
                        }
                    }
                    //check whether we have an apostry ending
                    if (wordpos > 1)
                    {
                        if (wordbuffer[wordpos - 2] == 39)
                        {
                            switch (wordbuffer[wordpos - 1])
                            {
                                case 't':
                                    if (wordpos > 2)
                                    {
                                        if (wordbuffer[wordpos - 3] == 'n')
                                        {
                                            wordpos -= 3;
                                        }
                                    }
                                    break;
                                case 'e':
                                    if (wordpos > 2)
                                    {
                                        if ((wordbuffer[wordpos - 3] == 'r') || (wordbuffer[wordpos - 3] == 'v'))
                                        {
                                            wordpos -= 3;
                                        }
                                    }
                                    break;
                                case 'l':
                                    if (wordpos > 2)
                                    {
                                        if (wordbuffer[wordpos - 3] == 'l')
                                        {
                                            wordpos -= 3;
                                        }
                                    }
                                    break;
                                case 's':
                                    wordpos -= 2;
                                    break;
                                case 'd':
                                    wordpos -= 2;
                                    break;
                                case 'm':
                                    wordpos -= 2;
                                    break;
                            }
                        }
                    }
                }
                return false;
            }
            //check if the letter is a COSMOS formatting character
            if (letter == '#')
            {
                //check if we have a Cosmos formatting character
                if (wordpos > 1)
                {
                    if ((wordbuffer[wordpos - 1] == 'N') && (wordbuffer[wordpos - 1] == '#'))
                    {
                        //Cosmos line break
                        wordpos -= 2;
                        return false;
                    }
                    if ((wordbuffer[wordpos - 1] == 'R') && (wordbuffer[wordpos - 1] == '#'))
                    {
                        //Cosmos \r
                        wordpos -= 2;
                        return false;
                    }
                }
                if (wordpos > 3)
                {
                    if (wordbuffer[wordpos - 3] == '#')
                    {
                        if ((wordbuffer[wordpos - 2] == 'T') && (wordbuffer[wordpos - 1] == 'A') && (wordbuffer[wordpos] == 'B'))
                        {
                            wordpos -= 4;
                            return false;
                        }
                    }
                }
            }
            //check if previous letter is an apostrophy - in this case 
            if (wordpos >= bufferlength)
            {
                word_overflow = true;
                return true;
            }
            wordbuffer[wordpos] = letter;
            wordpos++;
            return true;
        }

        //this method reduces special HTML characters
        public void ReduceSpecialCharacter(ref char letter)
        {
            //there are 3 possibilities 
            // &text;
            // &#decimal;
            // &#xhexadecimal;
            //start by finding last & character
            int offset = 0;
            bool foundamp = false;
            for (offset = 0; offset <= wordpos; offset++)
            {
                if (wordbuffer[wordpos - offset] == '&')
                {
                    foundamp = true;
                    break;
                }
            }
            if (!foundamp)
            {
                return;
            }
            char replacement = ' ';
            if (wordpos - offset + 1 >= bufferlength)
            {
                return;
            }
            if (wordbuffer[wordpos - offset + 1] == '#')
            {
                if (wordpos - offset + 2 >= bufferlength)
                {
                    return;
                }
                if (wordbuffer[wordpos - offset + 2] == 'x')
                {
                    //hexa-decimal
                    if (found_numeric_entity(true, wordpos - offset + 3, wordpos, ref replacement))
                    {
                        wordpos -= offset;
                        letter = replacement;
                        return;
                    }
                }
                else
                {
                    //decimal
                    if (found_numeric_entity(false, wordpos - offset + 2, wordpos, ref replacement))
                    {
                        wordpos -= offset;
                        letter = replacement;
                        return;
                    }
                }
            }
            else
            {
                //check if we found a character entity
                if (found_char_entity(wordpos - offset + 1, wordpos, ref replacement))
                {
                    if (replacement == 'X')
                    {
                        replacement = '"';
                    }
                    if (replacement == 'W')
                    {
                        replacement = ' ';
                    }
                    if (replacement == 'Z')
                    {
                        replacement = (char)0;
                    }
                    wordpos -= offset;
                    letter = replacement;
                    return;
                }
            }
        }
        public bool found_numeric_entity(bool hexmode, int startpos, int endpos, ref char replacement)
        {
            //ignore if the numeric entity has zero length
            if (endpos <= startpos)
            {
                return false;
            }
            char[] entity = new char[endpos - startpos];
            for (int i = 0; i < entity.Length; i++)
            {
                entity[i] = wordbuffer[startpos + i];
            }
            string entitystring = new String(entity);
            if (hexmode)
            {
                int number;
                bool result = Int32.TryParse(entitystring, System.Globalization.NumberStyles.HexNumber, null, out number);

                if (result)
                {
                    //convert to unicode
                    try
                    {
                        replacement = (char)number;
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                //decimal mode
                int number;
                bool result = Int32.TryParse(entitystring, out number);
                if (result)
                {
                    //convert to unicode
                    try
                    {
                        replacement = (char)number;
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            return false;
        }
        public bool found_char_entity(int startpos, int endpos, ref char replacement)
        {
            //ignore if the char entity has zero length
            if (endpos <= startpos)
            {
                return false;
            }
            char[] entity = new char[endpos - startpos];
            for (int i = 0; i < entity.Length; i++)
            {
                entity[i] = wordbuffer[startpos + i];
            }
            string entitystring = new String(entity);
            if (specialhtml_dict.ContainsKey(entitystring))
            {
                replacement = specialhtml_dict[entitystring];
                return true;
            }
            else
            {
                return false;
            }
        }

        //this method checks whether the wordbuffer contains a valid ASCII string or pure number
        //also converts to lower case in the process
        public bool ValidAsciiOrInteger()
        {
            hyphenationcounter = 0;
            if (word_overflow)
            {
                return false;
            }
            if ((wordpos == 0) || (wordpos == 1 && wordbuffer[0] == ' '))
            {
                return false;
            }
            //ignore words with one letter
            //if (wordpos <= 1)
            //{
            //    return false;
            //}
            //remove diacritics before adding the letter
            try
            {
                char[] dummy = new char[wordpos];
                for (int i = 0; i < wordpos; i++)
                {
                    dummy[i] = wordbuffer[i];
                }
                string dummystring = new string(dummy);
                string composite = dummystring.Normalize(NormalizationForm.FormKC);
                if (composite.Length <= wordbuffer.Length)
                {
                    wordpos = composite.Length;
                }
                else
                {
                    return false;
                }
                for (int i = 0; i < wordpos; i++)
                {
                    string norm = composite[i].ToString().Normalize(NormalizationForm.FormKD);
                    wordbuffer[i] = norm[0];
                }
            }
            catch
            {
                return false;
            }

            bool containsASCII = false;
            bool containsInt = false;
            for (int i = 0; i < wordpos; i++)
            {
                //Console.Write(wordbuffer[i]);
                if (wordbuffer[i] >= 65 && wordbuffer[i] <= 90)
                {
                    //turn into lower-case letter 
                    wordbuffer[i] = (char)((int)wordbuffer[i] + 32);
                    containsASCII = true;
                }
                else
                {
                    if (wordbuffer[i] >= 97 && wordbuffer[i] <= 122)
                    {
                        //ok - already lower-case letter
                        containsASCII = true;
                    }
                    else
                    {
                        if (wordbuffer[i] >= 48 && wordbuffer[i] <= 57)
                        {
                            //ok - this is a number
                            containsInt = true;
                        }
                        else
                        {
                            if (wordbuffer[i] != '-')
                            {
                                return false;
                            }
                            else
                            {
                                //hyphenation
                                hyphenations[hyphenationcounter] = i;
                                if (containsInt && !containsASCII)
                                {
                                    isInteger[hyphenationcounter] = true;
                                }
                                else
                                {
                                    isInteger[hyphenationcounter] = false;
                                }
                                containsASCII = false;
                                containsInt = false;
                                if (hyphenationcounter == 0)
                                {
                                    if (i <= 1)
                                    {
                                        return false;
                                    }
                                }
                                else
                                {
                                    if (hyphenations[hyphenationcounter] - hyphenations[hyphenationcounter - 1] <= 1)
                                    {
                                        return false;
                                    }
                                }
                                hyphenationcounter++;
                                if (hyphenationcounter >= hyphenations.Length)
                                {
                                    return false;
                                }
                            }
                        }
                    }
                }
                //hyphen on last character indicates word-seperation - ignore them
                if (wordbuffer[i] == '-')
                {
                    if (i == wordpos - 1)
                    {
                        return false;
                    }
                    //same when on first position
                    if (i == 0)
                    {
                        return false;
                    }
                }
            }
            if (containsInt && !containsASCII)
            {
                isInteger[hyphenationcounter] = true;
            }
            else
            {
                isInteger[hyphenationcounter] = false;
            }
            return true;
        }
        public bool GetStemmedCharArrays(bool ignoreintegers, ref int hcounter, ref char[] chararray, ref int offset, ref int length, ref bool isInt, ref int decodedInt)
        {
            isInt = false;
            if (hcounter > hyphenationcounter)
            {
                return false;
            }
            if (hyphenationcounter == 0)
            {
                if (isInteger[0])
                {
                    //we have a pure integer
                    if (ignoreintegers)
                    {
                        decodedInt = -1;
                        hcounter++;
                        isInt = true;
                        return true;
                    }
                    if (Int32.TryParse(new string(wordbuffer, 0, wordpos), out decodedInt))
                    {
                        if (decodedInt < 0 || decodedInt > 65535)
                        {
                            decodedInt = -1;
                        }
                    }
                    else
                    {
                        decodedInt = -1;
                    }
                    hcounter++;
                    isInt = true;
                    return true;
                }
                else
                {
                    porterstemmer.add(wordbuffer, wordpos);
                    porterstemmer.stem();
                    chararray = porterstemmer.getResultBuffer();
                    length = porterstemmer.getResultLength();
                    offset = 0;
                    hcounter++;
                    return true;
                }
            }
            else
            {
                if (hcounter < hyphenationcounter)
                {
                    if (isInteger[hcounter])
                    {
                        //we have an integer
                        if (ignoreintegers)
                        {
                            offset = hyphenations[hcounter] + 1;
                            decodedInt = -1;
                            hcounter++;
                            isInt = true;
                            return true;
                        }
                        if (Int32.TryParse(new string(wordbuffer, offset, hyphenations[hcounter] - offset), out decodedInt))
                        {
                            if (decodedInt < 0 || decodedInt > 65535)
                            {
                                decodedInt = -1;
                            }
                        }
                        else
                        {
                            decodedInt = -1;
                        }
                        offset = hyphenations[hcounter] + 1;
                        isInt = true;
                        hcounter++;
                        return true;
                    }
                    else
                    {
                        porterstemmer.add(wordbuffer, offset, hyphenations[hcounter] - offset);
                        porterstemmer.stem();
                        offset = hyphenations[hcounter] + 1;
                        chararray = porterstemmer.getResultBuffer();
                        length = porterstemmer.getResultLength();
                        hcounter++;
                        return true;
                    }
                    //Console.WriteLine("word {0}: {1}", i, output[i]);
                }
                //finally add the last part
                if (isInteger[hcounter])
                {
                    //we have an integer
                    if (ignoreintegers)
                    {
                        decodedInt = -1;
                        hcounter++;
                        isInt = true;
                        return true;
                    }
                    if (Int32.TryParse(new string(wordbuffer, offset, wordpos - offset), out decodedInt))
                    {
                        if (decodedInt < 0 || decodedInt > 65535)
                        {
                            decodedInt = -1;
                        }
                    }
                    else
                    {
                        decodedInt = -1;
                    }
                    hcounter++;
                    isInt = true;
                    return true;
                }
                else
                {
                    porterstemmer.add(wordbuffer, offset, wordpos - offset);
                    porterstemmer.stem();
                    chararray = porterstemmer.getResultBuffer();
                    length = porterstemmer.getResultLength();
                    hcounter++;
                    return true;
                }
                //Console.WriteLine("word {0}: {1}", hyphenationcounter, output[hyphenationcounter]);
            }
        }
    }
}
