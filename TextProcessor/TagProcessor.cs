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
    //the tag processor is fed characters and can identify tags
    public class TagProcessor
    {
        //these tags, once opened, have to be closed before text is accepted
        //"head" is not included since head tags can be optionally closed
        struct singletag
        {
            public string tag;
            public bool hidden;
            public bool sticky;
            public bool division;
        }

        singletag[] tags = { new singletag() { tag = "style", hidden = true, sticky = false, division = false }, new singletag() { tag = "select", hidden = true, sticky = false, division = false }, new singletag() { tag = "script", hidden = true, sticky = false, division = false }, new singletag() { tag = "object", hidden = true, sticky = false, division = false }, new singletag() { tag = "embed", hidden = true, sticky = false, division = false }, new singletag() { tag = "applet", hidden = true, sticky = false, division = false }, new singletag() { tag = "noframes", hidden = true, sticky = false, division = false }, new singletag() { tag = "noscript", hidden = true, sticky = false, division = false }, new singletag() { tag = "noembed", hidden = true, sticky = false, division = false }, new singletag() { tag = "title", hidden = false, sticky = true, division = true }, new singletag() { tag = "div", hidden = false, sticky = false, division = true }, new singletag() { tag = "math", hidden = true, sticky = false, division = false }, new singletag() { tag = "ref", hidden = true, sticky = false, division = false } };
        bool[] tag_match;
        bool nomatch;
        int pos;
        public Dictionary<string, int> tagidlist;
        public Dictionary<int, string> tagidtrans;

        public TagProcessor()
        {
            tag_match = new bool[tags.Length];
            //now create tagidlist
            tagidlist = new Dictionary<string, int>();
            tagidtrans = new Dictionary<int, string>();
            for (int i = 0; i < tags.Length; i++)
            {
                if (!tagidlist.ContainsKey(tags[i].tag))
                {
                    tagidlist.Add(tags[i].tag, i);
                    tagidtrans.Add(i, tags[i].tag);
                }
            }
        }

        public void initialize()
        {
            for (int i = 0; i < tags.Length; i++)
            {
                tag_match[i] = true;
            }
            nomatch = false;
            pos = 0;
        }
        public bool AddLetter(char letter)
        {
            //Console.Write(letter.ToString().ToUpper());
            //map to ascii
            if (letter >= 65 && letter <= 90)
            {
                //turn into lower-case letter 
                letter = (char)((int)letter + 32);
            }
            else
            {
                if (letter >= 97 && letter <= 122)
                {
                    //ok - already lower-case letter
                }
                else
                {
                    //check if there is a space or '>' following
                    if (letter == ' ')
                    {
                        return false;
                    }
                    //non-ascii character
                    nomatch = true;
                    return true;
                }
            }
            //add ascii character
            if (nomatch)
            {
                return true;
            }
            int num_matches = 0;
            for (int i = 0; i < tags.Length; i++)
            {
                if (tag_match[i])
                {
                    if (tags[i].tag.Length <= pos)
                    {
                        tag_match[i] = false;
                    }
                    else
                    {
                        if (tags[i].tag[pos] == letter)
                        {
                            num_matches++;
                        }
                        else
                        {
                            tag_match[i] = false;
                        }
                    }
                }
            }
            if (num_matches == 0)
            {
                nomatch = true;
            }
            pos++;
            return true;
        }
        public bool GetTag(out int tagid, out bool hidden, out bool sticky, out bool division)
        {
            tagid = -1;
            hidden = false;
            division = false;
            sticky = false;
            if (nomatch)
            {
                return false;
            }
            else
            {
                for (int i = 0; i < tags.Length; i++)
                {
                    if ((tag_match[i]) && (tags[i].tag.Length == pos))
                    {
                        tagid = i;
                        hidden = tags[i].hidden;
                        sticky = tags[i].sticky;
                        division = tags[i].division;
                        return true;
                    }
                }
                return false;
            }
        }

    }
}
