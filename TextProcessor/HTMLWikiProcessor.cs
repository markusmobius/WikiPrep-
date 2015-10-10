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

    public class HTMLWikiProcessor
    {
        TagProcessor tp;
        BufferProcessor bp;
        WordProcessor wp;
        WikiProcessor wikip;
        HashSet<int> exclusions;
        char[] extrawords;
        int extraword_counter;
        bool lastwordstopstatus;
        bool ignoreintegers;

        DecodedTextClass content;

        int currentdivision;

        public HTMLWikiProcessor(HashSet<int> exclusions, bool ignoreintegers)
        {
            this.exclusions = exclusions;
            this.ignoreintegers = ignoreintegers;
            //initialize Tag processor and buffer
            tp = new TagProcessor();
            bp = new BufferProcessor(20);
            wp = new WordProcessor(150);
            wikip = new WikiProcessor();
            extrawords = new char[500];
            extraword_counter = 0;
        }

        public void LoadDecodedTextClass(ref DecodedTextClass content)
        {
            this.content = content;
        }

        public void ProcessHTML(string html)
        {
            currentdivision = 0;
            bp.initialize();
            extraword_counter = 0;
            lastwordstopstatus = false;

            //now cycle through the html document
            //mode 0 is outside tag, mode 1 is inside tag
            bool tagmode = false;
            bool wikimode = false;
            bool wikimode_curly = false;
            wp.initialize();
            bool openingtag = false;
            bool calltagprocessor = false;
            int currenthiddentag = -1;
            int currentstickytag = -1;
            bool issticky, isdivision, ishidden;
            int currenttag = -1;

            int poscounter = 0;
            int extraword_i = 0;
            while (extraword_counter > 0 || poscounter <= html.Length)
            {
                //Console.Write(html[poscounter]);
                char letter;
                if (extraword_counter > 0)
                {
                    letter = extrawords[extraword_i];
                    extraword_i++;
                    if (extraword_i == extraword_counter)
                    {
                        extraword_counter = 0;
                        extraword_i = 0;
                    }
                }
                else
                {
                    if (poscounter < html.Length)
                    {
                        letter = html[poscounter];
                    }
                    else
                    {
                        letter = ' ';
                    }
                    poscounter++;
                }
                //check for the wikimedia special cases first
                if (content.wikimedia)
                {
                    if (letter == '{' && bp.GetPastLetter(-1) == '{' && !wikimode)
                    {
                        wikimode = true;
                        wikimode_curly = true;
                        wikip.reset(wikimode_curly);
                        bp.AddLetter(letter);
                        continue;
                    }
                    if (letter == '[' && bp.GetPastLetter(-1) == '[' && !wikimode)
                    {
                        wikimode = true;
                        wikimode_curly = false;
                        wikip.reset(wikimode_curly);
                        bp.AddLetter(letter);
                        continue;
                    }
                    if (wikimode && wikimode_curly && letter == '}' && bp.GetPastLetter(-1) == '}')
                    {
                        //found a curly segment
                        wikip.Close();
                        AddWikiSegments();
                        bp.AddLetter(letter);
                        wikimode = false;
                        continue;
                    }
                    if (wikimode && !wikimode_curly && letter == ']' && bp.GetPastLetter(-1) == ']')
                    {
                        //found an edgy segment
                        wikip.Close();
                        AddWikiSegments();
                        bp.AddLetter(letter);
                        wikimode = false;
                        continue;
                    }
                    if (wikimode)
                    {
                        //add letter to buffer
                        wikip.AddLetter(letter); //wikiprocessor
                        bp.AddLetter(letter); //bufferprocessor
                        continue;
                    }
                }
                //now deal with other html code
                switch (letter)
                {
                    case '<':
                        //tag was opened
                        //check if there is a word to be written
                        wp.AddLetter(' ');
                        if (wp.ValidAsciiOrInteger())
                        {
                            AddWords(false, currentdivision, currentstickytag, currenthiddentag);
                        }
                        else
                        {
                            AddWords(true, currentdivision, currentstickytag, currenthiddentag);
                        }
                        tagmode = true;
                        openingtag = true;
                        calltagprocessor = true;
                        tp.initialize();
                        break;
                    case '/':
                        if (tagmode)
                        {
                            if (bp.GetPastLetter(-1) == '<')
                            {
                                openingtag = false;
                            }
                            break;
                        }
                        goto default;
                    case '>':
                        if (calltagprocessor)
                        {
                            if (!tp.AddLetter(' '))
                            {
                                //tag ended
                                tp.GetTag(out currenttag, out ishidden, out issticky, out isdivision);
                                //hidden tag
                                if ((ishidden) && (currenthiddentag == -1) && openingtag)
                                {
                                    currenthiddentag = currenttag;
                                }
                                //sticky tag
                                if ((issticky) && (currentstickytag == -1) && openingtag)
                                {
                                    currentstickytag = currenttag;
                                    currentdivision++;
                                }
                                //division tag
                                if (isdivision)
                                {
                                    currentdivision++;
                                }
                                calltagprocessor = false;
                            }
                        }
                        //tag was closed
                        if (bp.GetPastLetter(-1) == '/')
                        {
                            openingtag = false;
                        }
                        //did we just close a hidden tag?
                        if ((currenthiddentag == currenttag) && (!openingtag) && tagmode)
                        {
                            currenthiddentag = -1;
                        }
                        //did we just close a sticky tag?
                        if ((currentstickytag == currenttag) && (!openingtag) && tagmode)
                        {
                            currentstickytag = -1;
                        }
                        tagmode = false;
                        openingtag = false;
                        calltagprocessor = false;
                        wp.initialize();
                        break;
                    default:
                        if (tagmode)
                        {
                            //we are inside a tag
                            if (calltagprocessor)
                            {
                                if (!tp.AddLetter(letter))
                                {
                                    //tag ended
                                    tp.GetTag(out currenttag, out ishidden, out issticky, out isdivision);
                                    //hidden tag
                                    if ((ishidden) && (currenthiddentag == -1) && openingtag)
                                    {
                                        currenthiddentag = currenttag;
                                    }
                                    //sticky tag
                                    if ((issticky) && (currentstickytag == -1) && openingtag)
                                    {
                                        currentstickytag = currenttag;
                                        currentdivision++;
                                    }
                                    //division tag
                                    if (isdivision)
                                    {
                                        currentdivision++;
                                    }
                                    calltagprocessor = false;
                                }
                            }
                        }
                        else
                        {
                            //let's try to add the letter to the current word
                            if (currenthiddentag == -1)
                            {
                                //if (letter == ' ')
                                //{
                                //    Console.Write("hello");
                                //}
                                //AddedLetter is false if letter is space, \n, \r, \t
                                bool AddedLetter = wp.AddLetter(letter);
                                //Console.WriteLine(AddedLetter);
                                if (!AddedLetter)
                                {
                                    //word has finished
                                    if (wp.ValidAsciiOrInteger())
                                    {
                                        AddWords(false, currentdivision, currentstickytag, currenthiddentag);
                                    }
                                    else
                                    {
                                        AddWords(true, currentdivision, currentstickytag, currenthiddentag);
                                    }
                                    //reinitialize word processor
                                    wp.initialize();
                                }
                            }
                        }
                        break;
                }
                //add letter to buffer
                bp.AddLetter(letter);
            }
        }

        int[] tag_hierarchy = new int[100];
        public void ProcessDivHTML(string html, HashSet<string> divfilters)
        {
            currentdivision = 0;
            int depth = 0;
            bp.initialize();
            extraword_counter = 0;
            lastwordstopstatus = false;

            //extra flags 
            bool div_matchflag = false;
            bool div_matchflag_deep = false;
            int div_reading = 0;
            int div_depth = 0;
            string div_tagname = "";
            int div_tagid = tp.tagidlist["div"];

            //now cycle through the html document
            //mode 0 is outside tag, mode 1 is inside tag
            bool tagmode = false;
            wp.initialize();
            bool openingtag = false;
            bool calltagprocessor = false;
            int currenthiddentag = -1;
            int currentstickytag = -1;
            bool issticky, isdivision, ishidden;
            int currenttag = -1;

            int poscounter = 0;
            int extraword_i = 0;
            while (extraword_counter > 0 || poscounter <= html.Length)
            {
                //Console.Write(html[poscounter]);
                char letter;
                if (extraword_counter > 0)
                {
                    letter = extrawords[extraword_i];
                    extraword_i++;
                    if (extraword_i == extraword_counter)
                    {
                        extraword_counter = 0;
                        extraword_i = 0;
                    }
                }
                else
                {
                    if (poscounter < html.Length)
                    {
                        letter = html[poscounter];
                    }
                    else
                    {
                        letter = ' ';
                    }
                    poscounter++;
                }
                //now deal with other html code
                switch (letter)
                {
                    case '<':
                        //tag was opened
                        //check if there is a word to be written
                        wp.AddLetter(' ');
                        if (wp.ValidAsciiOrInteger())
                        {
                            if ((currentstickytag != -1) || div_matchflag_deep)
                            {
                                AddWords(false, currentdivision, currentstickytag, currenthiddentag);
                            }
                        }
                        else
                        {
                            if ((currentstickytag != -1) || div_matchflag_deep)
                            {
                                AddWords(true, currentdivision, currentstickytag, currenthiddentag);
                            }
                        }
                        tagmode = true;
                        openingtag = true;
                        calltagprocessor = true;
                        tp.initialize();
                        break;
                    case '/':
                        if (tagmode)
                        {
                            if (bp.GetPastLetter(-1) == '<')
                            {
                                openingtag = false;
                            }
                            break;
                        }
                        goto default;
                    case '>':
                        if (calltagprocessor)
                        {
                            if (!tp.AddLetter(' '))
                            {
                                //tag ended
                                tp.GetTag(out currenttag, out ishidden, out issticky, out isdivision);
                                //hidden tag
                                if ((ishidden) && (currenthiddentag == -1) && openingtag)
                                {
                                    currenthiddentag = currenttag;
                                }
                                //sticky tag
                                if ((issticky) && (currentstickytag == -1) && openingtag)
                                {
                                    currentstickytag = currenttag;
                                    currentdivision++;
                                }
                                //division tag
                                if (isdivision)
                                {
                                    currentdivision++;
                                }
                                calltagprocessor = false;
                            }
                        }
                        //tag was closed
                        if (bp.GetPastLetter(-1) == '/')
                        {
                            openingtag = false;
                        }
                        else
                        {
                            if (openingtag)
                            {
                                //check if we have a div match
                                switch (div_reading)
                                {
                                    case 1:
                                        //Console.WriteLine(div_tagname);
                                        if (divfilters.Contains(div_tagname) && (depth < tag_hierarchy.Length) && (!div_matchflag))
                                        {
                                            //Console.WriteLine("match! "+depth);
                                            div_matchflag = true;
                                            div_matchflag_deep = true;
                                            div_depth = depth + 1;
                                        }
                                        div_reading = 2;
                                        break;
                                    default:
                                        break;
                                }

                                if (currenttag == div_tagid)
                                {
                                    if (depth < tag_hierarchy.Length)
                                    {
                                        tag_hierarchy[depth] = currenttag;
                                    }
                                    //if (depth < 10)
                                    //{
                                    //Console.WriteLine(depth + " " + tp.tagidtrans[currenttag]+" open");
                                    //}
                                    depth++;
                                    if (div_matchflag)
                                    {
                                        div_matchflag_deep = true;
                                        if (depth > div_depth)
                                        {
                                            div_matchflag_deep = false;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                if (currenttag == div_tagid)
                                {
                                    depth--;
                                    //if (depth < 10)
                                    //{
                                    //Console.WriteLine(depth + " " + tp.tagidtrans[currenttag] + " closed");
                                    //}
                                    if ((depth < div_depth) && div_matchflag)
                                    {
                                        div_matchflag = false;
                                        div_matchflag_deep = false;
                                        //Console.WriteLine("match over");
                                    }
                                    if (div_matchflag)
                                    {
                                        div_matchflag_deep = true;
                                        if (depth > div_depth)
                                        {
                                            div_matchflag_deep = false;
                                        }
                                    }
                                }
                            }
                        }
                        //did we just close a hidden tag?
                        if ((currenthiddentag == currenttag) && (!openingtag) && tagmode)
                        {
                            currenthiddentag = -1;
                        }
                        //did we just close a sticky tag?
                        if ((currentstickytag == currenttag) && (!openingtag) && tagmode)
                        {
                            currentstickytag = -1;
                        }
                        tagmode = false;
                        openingtag = false;
                        calltagprocessor = false;
                        wp.initialize();
                        break;
                    default:
                        if (tagmode)
                        {
                            //we are inside a tag
                            if (calltagprocessor)
                            {
                                if (!tp.AddLetter(letter))
                                {
                                    //tag ended
                                    tp.GetTag(out currenttag, out ishidden, out issticky, out isdivision);
                                    div_tagname = "";
                                    div_reading = 0;
                                    //hidden tag
                                    if ((ishidden) && (currenthiddentag == -1) && openingtag)
                                    {
                                        currenthiddentag = currenttag;
                                    }
                                    //sticky tag
                                    if ((issticky) && (currentstickytag == -1) && openingtag)
                                    {
                                        currentstickytag = currenttag;
                                        currentdivision++;
                                    }
                                    //division tag
                                    if (isdivision)
                                    {
                                        currentdivision++;
                                    }
                                    calltagprocessor = false;
                                }
                            }
                            else
                            {
                                //do we have a divtag?
                                if ((currenttag == div_tagid) && (!div_matchflag))
                                {
                                    //first space starts accumulate mode
                                    switch (letter)
                                    {
                                        case ' ':
                                            switch (div_reading)
                                            {
                                                case 1:
                                                    //Console.WriteLine(div_tagname);
                                                    if (divfilters.Contains(div_tagname) && (depth < tag_hierarchy.Length))
                                                    {
                                                        div_matchflag = true;
                                                        div_matchflag_deep = true;
                                                        div_depth = depth;
                                                    }
                                                    div_reading = 2;
                                                    break;
                                                default:
                                                    break;
                                            }
                                            break;
                                        default:
                                            if (div_reading == 0)
                                            {
                                                div_reading = 1;
                                            }
                                            if (div_reading == 1)
                                            {
                                                div_tagname += letter;
                                            }
                                            break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            //let's try to add the letter to the current word
                            if (currenthiddentag == -1)
                            {
                                //if (letter == ' ')
                                //{
                                //    Console.Write("hello");
                                //}
                                bool AddedLetter = wp.AddLetter(letter);
                                //Console.WriteLine(AddedLetter);
                                if (!AddedLetter)
                                {
                                    //word has finished
                                    if (wp.ValidAsciiOrInteger())
                                    {
                                        //Console.Write("X"+depth);
                                        if ((currentstickytag != -1) || div_matchflag_deep)
                                        {
                                            //Console.Write("word added");
                                            AddWords(false, currentdivision, currentstickytag, currenthiddentag);
                                        }
                                    }
                                    else
                                    {
                                        if ((currentstickytag != -1) || div_matchflag_deep)
                                        {
                                            AddWords(true, currentdivision, currentstickytag, currenthiddentag);
                                        }
                                    }
                                    //reinitialize word processor
                                    wp.initialize();
                                }
                            }
                        }
                        break;
                }
                //add letter to buffer
                bp.AddLetter(letter);
            }
        }

        public void AddWikiSegments()
        {
            char[] chararray = new char[0];
            int offset = 0;
            int length = 0;
            int type = 0;
            int counter = 0;
            bool setextraword = false;
            extraword_counter = 0;
            int extracounter = 0;
            while (wikip.GetSegment(counter, ref chararray, ref offset, ref length, ref type, ref extrawords, ref extracounter, ref setextraword))
            {
                if (length > 0)
                {
                    content.AddWikiConstruct(chararray, offset, length, type);
                }
                if (setextraword)
                {
                    extraword_counter = extracounter;
                    //Console.WriteLine(extraword_counter);
                    //Console.WriteLine(new string(extrawords,0,extraword_counter));
                }
                counter++;
            }
        }

        public void AddWords(bool stopword, int currentdivision, int currentstickytag, int currenthiddentag)
        {
            if (currenthiddentag != -1) { return; }
            char[] chararray = new char[0];
            int offset = 0;
            int length = 0;
            int hcounter = 0;
            bool isInt = false;
            int decodedInt = -1;
            while (wp.GetStemmedCharArrays(ignoreintegers, ref hcounter, ref chararray, ref offset, ref length, ref isInt, ref decodedInt))
            {
                //create word hash
                int hash = 7;
                for (int k = offset; k < offset + length; k++)
                {
                    hash = (hash * 17) + (chararray[k] | (chararray[k] << 0x10));
                }

                bool stickiness = true;
                if (currentstickytag == -1)
                {
                    stickiness = false;
                }
                if (stopword || exclusions.Contains(hash) || (isInt && decodedInt == -1))
                {
                    //we have a stopword - only add it if the previous word was not a stopword
                    if (!lastwordstopstatus)
                    {
                        content.AddWord(chararray, 0, stickiness, currentdivision, true, isInt, decodedInt);
                        lastwordstopstatus = true;
                    }
                }
                else
                {
                    content.AddWord(chararray, length, stickiness, currentdivision, false, isInt, decodedInt);
                    lastwordstopstatus = false;
                    //Console.WriteLine(new string(chararray,0,length));
                }
            }
        }
    }
}
