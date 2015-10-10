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
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using TextProcessor;

namespace WikiPrep
{
    public class WikiData
    {
        public class conceptstats
        {
            public bool valid;
            public HashSet<string> outlinks;
            public HashSet<string> inlinks;
            public HashSet<string> categories;
            public HashSet<string> redirects;
            public string title;
            public int[] titleArray;
            public int[] conceptwords;

            public conceptstats()
            {
                outlinks = new HashSet<string>();
                inlinks = new HashSet<string>();
                valid = false;
            }
        }

        // data structures
        public ConcurrentDictionary<int, conceptstats> conceptdata;
        public Dictionary<string, int> conceptdict;
        public Dictionary<string, string> conceptredirects;
        //structure to store concept texts
        public ConcurrentDictionary<string, int> worddict;
        public ConcurrentDictionary<int, int> wordidf; // stores the number of concepts a word appears in

        // structure to store category information
        public ConcurrentDictionary<string,int> categorydict;
        public ConcurrentDictionary<string,int[]> categoryTitleArray;
        public ConcurrentDictionary<string, HashSet<string>> parentcategorydict;

        // structure to store ambiguous strings and redirects
        public ConcurrentDictionary<string, HashSet<string>> disambigredirect;

        int numthreads;
        int[] threadcounters;

        public WikiData(int numthreads)
        {
            conceptdata = new ConcurrentDictionary<int, conceptstats>();
            conceptdict = new Dictionary<string, int>();
            conceptredirects = new Dictionary<string, string>();
            categorydict = new ConcurrentDictionary<string, int>();
            categoryTitleArray = new ConcurrentDictionary<string, int[]>();
            parentcategorydict = new ConcurrentDictionary<string, HashSet<string>>();
            disambigredirect = new ConcurrentDictionary<string, HashSet<string>>();
            worddict = new ConcurrentDictionary<string, int>();
            wordidf = new ConcurrentDictionary<int, int>();
            this.numthreads = numthreads;
            threadcounters = new int[numthreads];
        }

        public static string cleancategorytitle(string cattitle, bool catopen = false)
        {
            char[] result = new char[cattitle.Length];
            int counter = 0;
            bool firstletter = false;
            if (catopen)
                firstletter = true;
            bool prevempty = false;
            int lastnonempty = 0;

            int length = cattitle.Length;
            for (int i = 0; i < length; ++i)
            {
                if (!catopen && cattitle[i] == ':')
                {
                    catopen = true;
                    firstletter = true;
                    continue;
                }
                if (firstletter)
                {
                    if (cattitle[i] == ' ')
                        continue;

                    result[counter] = Char.ToUpper(cattitle[i]);
                    ++counter;
                    lastnonempty = counter;
                    firstletter = false;
                } // can deal here with <!--   --> and &amp;ndash;
                else if (catopen) // main section
                {
                    if (cattitle[i] == '_' || cattitle[i] == ' ') // if previous one was a space or underscore
                    {
                        if (prevempty)
                        {
                            continue;
                        }
                        else
                        {
                            prevempty = true;
                            result[counter] = ' ';
                            ++counter;
                        }

                    }
                    else
                    {
                        prevempty = false;
                        result[counter] = cattitle[i];
                        ++counter;
                        lastnonempty = counter;
                    }
                }
            }

            return new string(result, 0, lastnonempty);

        }
   
        public void process(ref DecodedTextClass res, int threadid, int link_threshold=5, int minimum_length=100)
        {
            if (res.isdisambig)
            {
                //Console.WriteLine(res.title);
                processdisambig(res);
            }
            else if (res.iscategory)
            {
                processcategory(res);
            }
            else if (res.redirect){
                if (!conceptredirects.ContainsKey(res.title) && res.title!=null && res.redirecttitle!=null)
                {
                    conceptredirects.Add(res.title, res.redirecttitle);
                }
            }
            else
            {
                // do a check for disambig
                if (isdisambig(res))
                {
                    res.isdisambig = true;
                    processdisambig(res);
                }
                else
                {
                    processregular(res, threadid, link_threshold, minimum_length);
                }
            }
        }

        private bool isdisambig(DecodedTextClass res)
        {
            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            int type = 0;

            List<int> categoriesarray = new List<int>(res.NumberWikiConstructs());
            string cat;
            int n = res.NumberWikiConstructs();
            for (int i = 0; i < n; i++)
            {
                if (res.GetWikiConstruct(i, ref type, ref chararray, ref startindex, ref length))
                {
                    switch (type)
                    {
                        case 1: // look at categories
                            cat = cleancategorytitle(new string(chararray, startindex, length), true); // already open because processor gets rid of "Category:"
                            if (cat == "Disambiguation pages")
                            {
                                //StreamWriter sw = new StreamWriter("complexdisambiguation.txt", true);
                                //sw.WriteLine(res.title + "," + res.identifier);
                                //sw.Close();
                                return true;
                            }
                            break;
                    }
                }
            }
            return false;
        }

        private void processcategory(DecodedTextClass res)
        {
            // structure to store category information
            //Dictionary<string, int> categorydict;
            //Dictionary<int, int[]> parentcategorydict;
            string modifiedtitle = cleancategorytitle(res.title, false); // string includes "category:"
            categorydict.TryAdd(modifiedtitle,res.identifier);

            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            int type = 0;

            HashSet<string> categoriesarray = new HashSet<string>();
            int n = res.NumberWikiConstructs();
            for (int i = 0; i < n; i++)
            {
                if (res.GetWikiConstruct(i, ref type, ref chararray, ref startindex, ref length))
                {
                    switch (type)
                    {
                        case 1:
                            string cat = cleancategorytitle(new string(chararray, startindex, length), true);
                            if (!categoriesarray.Contains(cat))
                            {
                                categoriesarray.Add(cat);
                            }
                            break;
                    }
                }
            }
            parentcategorydict.TryAdd(modifiedtitle,categoriesarray);
        }

        private void processdisambig(DecodedTextClass res)
        {
            HashSet<string> links = new HashSet<string>();

            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            int type = 0;

            int n = res.NumberWikiConstructs();
            for (int i = 0; i < n; i++)
            {
                if (res.GetWikiConstruct(i, ref type, ref chararray, ref startindex, ref length))
                {
                    switch (type)
                    {
                        case 0:
                            string curr = new string(chararray, startindex, length);
                            curr = curr.Trim();
                            if (!links.Contains(curr))
                            {
                                links.Add(curr);
                                //Console.WriteLine(curr);
                            }
                            break;
                    }
                }
            }
            //Console.WriteLine(res.title);
            disambigredirect.TryAdd(res.title, links);
        }

        private void processregular(DecodedTextClass res, int  threadid, int links_threshold = 5, int minimum_length=100)
        {
            string title = res.title.Trim();
            int id = res.identifier;
            conceptdata.TryAdd(id, new conceptstats());
            conceptdata[id].valid = true;

            //Console.WriteLine("Case 1");
            conceptdata[id].title = title;
            HashSet<string> categories = new HashSet<string>();
            HashSet<string> redirects = new HashSet<string>();

            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            int type = 0;

            int constructs = res.NumberWikiConstructs();

            if (constructs < links_threshold)
            {
                //invalidate this concept
                conceptdata[id].valid = false;
                return;
            }
            if (res.NumberGoodWords() < minimum_length)
            {
                //invalidate this concept
                conceptdata[id].valid = false;
                return;
            }

            string curr, cat, redir;
            for (int i = 0; i < constructs; i++)
            {
                if (res.GetWikiConstruct(i, ref type, ref chararray, ref startindex, ref length))
                {
                    switch (type)
                    {
                        case 0:
                            //Console.WriteLine("Case 0");
                            curr = new string(chararray, startindex, length);
                            curr = curr.Trim();
                            // add to outlinks
                            if (!conceptdata[id].outlinks.Contains(curr))
                            {
                               conceptdata[id].outlinks.Add(curr);
                            }
                            break;
                        case 1:
                            //Console.WriteLine("Case 1");
                            cat = cleancategorytitle(new string(chararray, startindex, length), true);
                            if (!categories.Contains(cat))
                            {
                                categories.Add(cat);
                            }
                            break;
                        case 2:
                            //Console.WriteLine("Case 2");
                            redir = new string(chararray, startindex, length);
                            if (!redirects.Contains(redir))
                            {
                                redirects.Add(redir);
                                //Console.WriteLine("{0}: {1}",res.title,redir);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }

            // set some of the other info
            conceptdata[id].categories = categories;
            conceptdata[id].redirects = redirects;

            //now process words
            processText(res,threadid);
            return;
        }

        private void processText(DecodedTextClass res, int threadid)
        {
            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            bool sticky = false;
            bool stopword = false;
            int division = 0;
            bool isInt = false;
            int decodedInt = -1;

            HashSet<int> added = new HashSet<int>();
            int len = res.NumberWords();

            string token;
            int[] stream = new int[len];
            int counter = 0;
            for (int i = 0; i < len; i++)
            {
                if (res.GetWord(i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt))
                {
                    if (!stopword && !isInt)
                    {
                        token = (new string(chararray, startindex, length));

                        int wordid = numthreads *threadcounters[threadid] + threadid;
                        if (!worddict.TryAdd(token, wordid))
                        {
                            wordid = worddict[token];
                        }
                        else // added
                        {
                            ++threadcounters[threadid];
                        }
                        stream[counter] = wordid;
                        ++counter;

                        // for figuring out whether the word has already been added in the current concept. If not, increment in the IDF count (number of concepts the word appears in)
                        if (!added.Contains(wordid))
                        {
                            if (!wordidf.TryAdd(wordid, 1))
                            {
                                ++wordidf[wordid];
                            }
                            added.Add(wordid);
                        }
                    }
                    else
                    {
                        //-1 indicates stopword/integer or other break of flow
                        if (isInt && decodedInt>0)
                        {
                            stream[counter] = -1-decodedInt;
                        }
                        else
                        {
                            stream[counter] = -1;
                        }
                        ++counter;
                    }
                }
            }

            // should work because id is unique
            conceptdata[res.identifier].conceptwords = stream;
        }

    }
}
