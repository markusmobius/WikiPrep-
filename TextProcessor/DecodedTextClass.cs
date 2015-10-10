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
using System.Threading;

namespace TextProcessor
{
    public class DecodedTextClass
    {
        //memory class for words and wikiconstructs
        public MemoryManager mem;
        //which thread it belongs to
        int threadid;

        //whether wikimedia xml or not
        public bool wikimedia;

        // stuff to do with wikimedia
        public int identifier;
        public string title;
        public bool iscategory;
        public bool isdisambig;
        public bool redirect;
        public string redirecttitle;

        public DecodedTextClass(MemoryManager mem, bool wikimedia = false)
        {
            //initialize memory
            this.mem = mem;
            threadid = mem.InitializeMemory();
            this.wikimedia = wikimedia;

            iscategory = false;
            isdisambig = false;

        }

        public void resetDecoder()
        {
            mem.ReleaseMemory(threadid);

        }

        public bool AddWord(char[] chararray, int chararraylength, bool sticky, int division, bool stopword, bool isInt, int decodedInt)
        {
            bool output = mem.AddWord(threadid, chararray, chararraylength, sticky, division, stopword, isInt, decodedInt);
            return output;
        }

        public bool AddWikiConstruct(char[] chararray, int offset, int chararraylength, int wikitype)
        {
            return mem.AddWikiConstruct(threadid, chararray, offset, chararraylength, wikitype);
        }

        // output methods
        public int NumberWords()
        {
            return mem.NumberWords(threadid);
        }
        public int NumberGoodWords()
        {
            return mem.NumberGoodWords(threadid);
        }

        public bool GetWord(int i, ref char[] chararray, ref int startindex, ref int length, ref bool sticky, ref int division, ref bool stopword, ref bool isInt, ref int decodedInt)
        {
            return mem.GetWord(threadid, i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt);
        }

        //get stream of integers representing the text in the processor - use supplied dictionary
        //stop words and words not in dictionary are interpreted as -1
        public int[] GetWordIntStream(Dictionary<string, int> worddict)
        {
            int[] output = new int[mem.NumberWords(threadid)];
            char[] chararray = new char[0];
            int length = 0;
            bool sticky = false;
            int division = 0;
            bool stopword = false;
            int startindex = 0;
            bool isInt = false;
            int decodedInt = -1;

            for (int i = 0; i < mem.NumberWords(threadid); i++)
            {
                mem.GetWord(threadid, i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt);


                if (isInt)
                {
                    if (decodedInt < 0)
                    {
                        output[i] = -1;
                    }
                    else
                    {
                        output[i] = -2 - decodedInt;
                    }
                }
                else if (stopword)
                {
                    output[i] = -1;
                }
                else
                {
                    string word = new string(chararray, startindex, length);

                    if (worddict.ContainsKey(word))
                    {
                        output[i] = worddict[word];
                    }
                    else
                    {
                        output[i] = -1;
                    }
                }
            }

            return output;
        }

        public string[] GetTokens()
        {
            string[] output = new string[mem.NumberWords(threadid)];

            char[] chararray = new char[0];
            int length = 0;
            bool sticky = false;
            int division = 0;
            bool stopword = false;
            int startindex = 0;
            bool isInt = false;
            int decodedInt = -1;

            for (int i = 0; i < mem.NumberWords(threadid); i++)
            {
                mem.GetWord(threadid, i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt);

                if (isInt)
                {
                    output[i] = "";
                }
                else if (stopword)
                {
                    output[i] = "";
                }
                else
                {
                    string word = new string(chararray, startindex, length);
                    output[i] = word;
                }
            }

            return output;
        }

        public string[] GetTitleTokens()
        {
            Dictionary<int, string> output = new Dictionary<int, string>();

            char[] chararray = new char[0];
            int length = 0;
            bool sticky = false;
            int division = 0;
            bool stopword = false;
            int startindex = 0;
            bool isInt = false;
            int decodedInt = -1;

            for (int i = 0; i < mem.NumberWords(threadid); i++)
            {
                mem.GetWord(threadid, i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt);
                if (sticky)
                {
                    if (isInt)
                    {
                        output.Add(output.Count, "");
                    }
                    else if (stopword)
                    {
                        output.Add(output.Count, "");
                    }
                    else
                    {
                        string word = new string(chararray, startindex, length);
                        output.Add(output.Count, word);
                    }
                }
            }
            return output.Values.ToArray<string>();
        }

        public string GetDebugStream()
        {
            string[] output = new string[mem.NumberWords(threadid)];

            char[] chararray = new char[0];
            int length = 0;
            bool sticky = false;
            int division = 0;
            bool stopword = false;
            int startindex = 0;
            bool isInt = false;
            int decodedInt = -1;

            for (int i = 0; i < mem.NumberWords(threadid); i++)
            {
                mem.GetWord(threadid, i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt);
                string row = "";
                if (isInt)
                {
                    row = decodedInt.ToString();
                }
                else if (stopword)
                {
                    row = "";
                }
                else
                {
                    row = new string(chararray, startindex, length);
                }
                row += " sticky: " + sticky.ToString();
                row += " division: " + division;
                output[i] = row;
            }

            return string.Join("\n", output);
        }

        //allowed types are links, categories and redirects
        public int NumberWikiConstructs()
        {
            return mem.NumberWikiConstructs(threadid);
        }

        //type==0 stands for link, 1 for category and 2 for redirect
        public bool GetWikiConstruct(int i, ref int wikitype, ref char[] wikiarray, ref int startindex, ref int length)
        {
            return mem.GetWikiConstruct(threadid, i, ref wikitype, ref wikiarray, ref startindex, ref length);
        }

    }
}
