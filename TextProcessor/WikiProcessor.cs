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
    public class WikiProcessor
    {

        char[] wikibuffer;
        int wikibuffer_counter = 0;
        bool wikimode_curly;
        bool overflow;
        bool badwiki;
        int partcounter;
        int[] offset;
        int currentoffset;
        int wikitype;
        char[] char_see;
        char[] char_redirect;
        char[] char_category;
        bool ignorechar;


        public WikiProcessor()
        {
            wikibuffer = new char[2000];
            partcounter = 0;
            offset = new int[20];
            currentoffset = 0;
            wikitype = 0;
            fillchar(ref char_see, "see");
            fillchar(ref char_redirect, "redirect");
            fillchar(ref char_category, "category");
        }

        public void reset(bool wikimode)
        {
            wikibuffer_counter = 0;
            currentoffset = 0;
            wikimode_curly = wikimode;
            badwiki = false;
            overflow = false;
            ignorechar = false;
            partcounter = 0;
            wikitype = 0;
        }

        public void fillchar(ref char[] chararray, string tagword)
        {
            chararray = new char[tagword.Length];
            chararray = tagword.ToCharArray(0, tagword.Length);
        }

        public bool comparechar(char[] sourcearray, int coffset, int length, char[] tagarray)
        {
            if (length != tagarray.Length)
            {
                return false;
            }
            for (int i = 0; i < length; i++)
            {
                char letter = sourcearray[coffset + i];
                //turn capital letter into small ones
                if (letter >= 65 && letter <= 90)
                {
                    //turn into lower-case letter 
                    letter = (char)((int)letter + 32);
                }
                if (letter != tagarray[i])
                {
                    return false;
                }
            }
            return true;
        }


        public bool AddLetter(char letter)
        {
            if (wikibuffer_counter >= wikibuffer.Length)
            {
                overflow = true;
                badwiki = true;
                return false;
            }
            if (partcounter > offset.Length)
            {
                badwiki = true;
            }
            if (badwiki)
            {
                return false;
            }
            //looking for pipe symbol
            if (letter == '|')
            {
                ignorechar = false;
                if (partcounter == 0)
                {
                    if (wikimode_curly)
                    {
                        if (comparechar(wikibuffer, 0, wikibuffer_counter, char_see))
                        {
                            //links
                            wikitype = 0;
                            offset[partcounter] = currentoffset;
                            currentoffset = wikibuffer_counter;
                            partcounter++;
                            return true;
                        }
                        if (comparechar(wikibuffer, 0, wikibuffer_counter, char_redirect))
                        {
                            //redirect
                            wikitype = 2;
                            offset[partcounter] = currentoffset;
                            currentoffset = wikibuffer_counter;
                            partcounter++;
                            return true;
                        }
                        badwiki = true;
                        return false;
                    }
                    else
                    {
                        wikitype = 0;
                        offset[partcounter] = currentoffset;
                        currentoffset = wikibuffer_counter;
                        partcounter++;
                        return true;
                    }
                }
                //partcounter>0
                if (partcounter < offset.Length - 1)
                {
                    offset[partcounter] = currentoffset;
                    partcounter++;
                }
                currentoffset = wikibuffer_counter;
                return true;
            }
            if (letter == ':')
            {
                if (partcounter == 0)
                {
                    if ((!wikimode_curly) && comparechar(wikibuffer, 0, wikibuffer_counter, char_category))
                    {
                        wikitype = 1;
                        offset[partcounter] = currentoffset;
                        currentoffset = wikibuffer_counter;
                        partcounter++;
                        return true;
                    }
                    badwiki = true;
                    return false;
                }
            }
            if (letter == '#')
            {
                if (!wikimode_curly)
                {
                    ignorechar = true;
                }
            }
            if ((letter == '[') || (letter == '{'))
            {
                badwiki = true;
            }
            if (ignorechar)
            {
                return true;
            }
            wikibuffer[wikibuffer_counter] = letter;
            wikibuffer_counter++;
            return true;
        }

        public bool Close()
        {
            offset[partcounter] = currentoffset;
            partcounter++;
            if (!ignorechar)
            {
                wikibuffer_counter--;
            }
            return true;
        }

        public bool GetSegment(int counter, ref char[] chararray, ref int segoffset, ref int length, ref int type, ref char[] extrawords, ref int extraword_counter, ref bool setextraword)
        {
            if (counter >= partcounter)
            {
                return false;
            }
            if (badwiki || overflow)
            {
                return false;
            }
            if (!wikimode_curly && partcounter > 3)
            {
                return false;
            }
            int seglength = GetLength(counter);
            //deal with extra words
            setextraword = false;
            if (!wikimode_curly && ((wikitype == 0 && ((counter == 0 && partcounter == 1) || (counter == 1 && partcounter == 2))) || (wikitype == 1 && counter == 2 && partcounter == 3)))
            {
                if (seglength <= extrawords.Length - 2)
                {
                    Array.Copy(wikibuffer, offset[counter], extrawords, 1, seglength);
                    extrawords[0] = (char)32;
                    extrawords[seglength + 1] = (char)32;
                    extraword_counter = seglength + 2;
                    setextraword = true;
                }
            }
            else
            {
                extraword_counter = 0;
            }
            //now deal with segments
            length = 0;
            if (!wikimode_curly)
            {
                switch (wikitype)
                {
                    case 0:
                        //we have a link
                        if (counter == 0)
                        {
                            chararray = wikibuffer;
                            segoffset = 0;
                            length = seglength;
                            type = wikitype;
                        }
                        break;
                    case 1:
                        //we have a category
                        if (counter == 1)
                        {
                            chararray = wikibuffer;
                            segoffset = offset[1];
                            length = seglength;
                            type = wikitype;
                        }
                        break;
                    default:
                        length = 0;
                        break;
                }
            }
            else
            {
                //we are in curly mode
                switch (wikitype)
                {
                    case 0:
                        //we have a link
                        if (counter > 0)
                        {
                            chararray = wikibuffer;
                            segoffset = offset[counter];
                            length = seglength;
                            type = wikitype;
                        }
                        break;
                    case 2:
                        //we have a redirect
                        if (counter > 0)
                        {
                            chararray = wikibuffer;
                            segoffset = offset[counter];
                            length = seglength;
                            type = wikitype;
                        }
                        break;
                    default:
                        length = 0;
                        break;
                }
            }
            return true;
        }

        public int GetLength(int part)
        {
            if (part < partcounter - 1)
            {
                return offset[part + 1] - offset[part];
            }
            else
            {
                if (part >= partcounter)
                {
                    return 0;
                }
                return wikibuffer_counter - offset[part];
            }
        }
    }
}
