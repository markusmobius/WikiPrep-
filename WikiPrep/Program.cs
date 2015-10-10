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
using Ionic.BZip2;
using Ionic.Zlib;
using System.Threading;
using TextProcessor;

/// Program that extracts WikiMedia snapshots of Wikipedia and saves relevant resource files

namespace WikiPrep
{
    class Program
    {
        static void help()
        {
            Console.WriteLine("");
            Console.WriteLine("-------------------------------------------------------------------------");
            Console.WriteLine("WIKIPREP -wiki <path>");
            Console.WriteLine("       [-threads <int>] [-wordthreshold <int>] [-bigramthreshold <int>] [-minconceptlength <int>] [-minconstructs <int>] [-debug]");
            Console.WriteLine("");
            Console.WriteLine("-wiki <path>: Denotes path to wikimedia file (bz2 encoding assumed)");
            Console.WriteLine("");
            Console.WriteLine("-threads <int>: Worker threads for wiki decoding (default is 2)");
            Console.WriteLine("");
            Console.WriteLine("-wordthreshold <int>: number of concepts a word has to appear in for inclusion (default is 3)");
            Console.WriteLine("");
            Console.WriteLine("-bigramthreshold <int>: number of concepts a bigram has to appear in for inclusion (default is 3)");
            Console.WriteLine("");
            Console.WriteLine("-minconceptlength <int>: minimum length of a concept for inclusion (default is 100)");
            Console.WriteLine("");
            Console.WriteLine("-minconstructs <int>: minimum inlinks/outlinks for a concept to be included (default is 5)");
            Console.WriteLine("");
            Console.WriteLine("-debug: Output extra debug information (optional)");
        }
        static void Main(string[] args)
        {

            //read parameters from commandline
            Arguments CommandLine = new Arguments(args);
            string wikipath;
            int numthreads;
            wikistream wikimediastream = new wikistream();

            if (CommandLine["help"] != null)
            {
                help();
                return;
            }

            if (CommandLine["wiki"] != null)
            {
                Console.WriteLine("Using wikimedia file: " + CommandLine["wiki"]);
                wikipath = CommandLine["wiki"];
            }
            else
            {
                Console.WriteLine("No wikimedia file provided!");
                return;
            }
            FileStream fileStreamIn;
            try
            {
                fileStreamIn = new FileStream(wikipath, FileMode.Open, FileAccess.Read);
            }
            catch
            {
                Console.WriteLine("Cannot access wikimedia file!");
                return;
            }
            //determine type of stream
            string[] els = wikipath.Split('.');
            switch (els[els.Length - 1])
            {
                case "xml":
                    wikimediastream.xmlstream = fileStreamIn;
                    wikimediastream.type = "xml";
                    break;
                case "gz":
                    try
                    {
                        wikimediastream.gzipstream = new GZipStream(fileStreamIn,CompressionMode.Decompress);
                        wikimediastream.type = "gz";
                    }
                    catch
                    {
                        Console.WriteLine("This gzipped wikimedia archive is invalid.");
                        return;
                    }
                    break;
                case "bz2":
                    try
                    {
                        wikimediastream.bzip2stream = new BZip2InputStream(fileStreamIn);
                        wikimediastream.type = "bz2";
                    }
                    catch
                    {
                        Console.WriteLine("This bzip2 wikimedia archive is invalid.");
                        return;
                    }
                    break;
                default:
                    Console.WriteLine("This wikimedia file seems to be neither an XML file nor a valid gzip or bzip2 archive.");
                    return;
            }
            numthreads = 2;
            if (CommandLine["threads"] != null)
            {
                try
                {
                    Console.WriteLine("Number of worker threads: " + CommandLine["threads"]);
                    numthreads = Convert.ToInt32(CommandLine["threads"]);
                }
                catch
                {
                    Console.WriteLine("Invalid number of worker threads (has to lie between 1 and 128)");
                    return;
                }
                if ((numthreads < 1) || (numthreads > 128))
                {
                    Console.WriteLine("Invalid number of worker threads (has to lie between 1 and 128)");
                    return;
                }
            }
            else
            {
                Console.WriteLine("Number of worker threads: 2 (default)");
            }
            int wordthreshold = 3;
            int bigramthreshold = 3;
            int minconceptlength = 100;
            int minconstructs = 5;
            if (CommandLine["wordthreshold"] != null)
            {
                if (!Int32.TryParse(CommandLine["wordthreshold"], out wordthreshold) || wordthreshold <=0)
                {
                    Console.WriteLine("Invalid wordthreshold (has to be positive integer)");
                    return;
                }
            }
            if (CommandLine["bigramthreshold"] != null)
            {
                if (!Int32.TryParse(CommandLine["bigramthreshold"], out bigramthreshold) || bigramthreshold <= 0)
                {
                    Console.WriteLine("Invalid bigramthreshold (has to be positive integer)");
                    return;
                }
            }
            if (CommandLine["minconceptlength"] != null)
            {
                if (!Int32.TryParse(CommandLine["minconceptlength"], out minconceptlength) || minconceptlength <= 0)
                {
                    Console.WriteLine("Invalid minceptlength (has to be positive integer)");
                    return;
                }
            }
            if (CommandLine["minconstructs"] != null)
            {
                if (!Int32.TryParse(CommandLine["minconstructs"], out minconstructs) || minconstructs <= 0)
                {
                    Console.WriteLine("Invalid minconstructs (has to be positive integer)");
                    return;
                }
            }
            bool debug = false;
            if (CommandLine["debug"] != null)
            {
                debug = true;
            }

            //now declare memory class and a number of decoder classes (plus queue for filled decoders)
            MemoryManager mem = new MemoryManager(4000000, 4000000);

            // objects associated with two numbering queues
            int processed_docs = 0;
            Object processedlock = new Object();
            Object debuglock = new Object();


            int activethreads = numthreads;

            //reader thread
            //this thread reads byte[] into pages queue one page at a time
            ConcurrentQueue<byte[]> pages = new ConcurrentQueue<byte[]>();
            bool reader_done = false;
            new Thread(delegate()
            {
                wikireader.read(wikimediastream, ref pages, ref reader_done);
            }).Start();


            //two numbering threads
            // misc processing thread
            WikiData wikidata = new WikiData(numthreads);

            //now start worker threads
            object lockthread = new object();
            int badpages = 0;

            Object threadlock = new Object();
            int threadcounter = 0;

            for (int t = 0; t < numthreads; t++)
            {
                new Thread(delegate()
                {
                    //this is the worker thread
                    DecodedTextClass element = new DecodedTextClass(mem, true);
                    HTMLWikiProcessor textproc = new HTMLWikiProcessor(new HashSet<int>(), false);
                    textproc.LoadDecodedTextClass(ref element);

                    int threadid;
                    lock (threadlock)
                    {
                        threadid = threadcounter;
                        ++threadcounter;
                    }
                    string lasttitle = "";
                    string lastbody = "";
                    string lastid = "";
                    while (1 == 1)
                    {
                        int status = 0;
                        while (1 == 1)
                        {
                            if (reader_done && status == 0)
                            {
                                status = 1;
                            }
                            //get new page
                            byte[] singlepage;
                            if (pages.TryDequeue(out singlepage))
                            {
                                string body = "";
                                string identifier = "";
                                string title = "";
                                bool redirect = false;
                                string redirecttitle = "";
                                //we only process off the queue one page at a time, so we don't have to worry about storing
                                //multiple titles, etc... -- only one of each per string at a time
                                //description of refs given in pageextractor.cs
                                PageExtractor.GetPage(singlepage, ref body, ref identifier, ref title, ref redirect, ref redirecttitle);
                                /*
                                lock (debuglock)
                                {
                                    raw.WriteLine(identifier + "," + title);
                                }
                                */
                                if (body != "" && identifier != "" && title != "")
                                {
                                    //element is the decodedtextclass object
                                    element.identifier = Convert.ToInt32(identifier);
                                    element.title = title;
                                    element.redirect = redirect;
                                    element.redirecttitle = redirecttitle;
                                    try
                                    {
                                        //releases memory
                                        element.resetDecoder();
                                        //textproc is the HTMLWikiProcessor object (has element attached)
                                        //element is ref in htmlwikiprocessor, so this modifies it
                                        textproc.ProcessHTML(body);
                                    }
                                    catch (Exception e)
                                    {
                                        lock (debuglock)
                                        {
                                            StreamWriter error = new StreamWriter("error.txt", true);
                                            error.WriteLine("ERROR PROCESSING PAGE");
                                            error.WriteLine("-------------");
                                            error.WriteLine(e.Message);
                                            error.WriteLine(e.StackTrace);
                                            error.WriteLine("-------------");
                                            error.Write(body);
                                            error.Close();
                                        }
                                        continue;
                                    }

                                    // determine the type of page by the title text
                                    if (title.EndsWith("(disambiguation)"))
                                    {
                                        element.isdisambig = true;
                                        element.title = title.Replace("(disambiguation)", " ").Trim();
                                    }
                                    else
                                    {
                                        element.isdisambig = false;
                                    }
                                    if (title.StartsWith("Category:"))
                                    {
                                        element.iscategory = true;
                                        element.title = title.Replace("Category:", " ").Trim();
                                    }
                                    else
                                    {
                                        element.iscategory = false;
                                    }
                                    // process the current element
                                    //element is the decodedtextclass object
                                    wikidata.process(ref element, threadid, minconstructs, minconceptlength);
                                    lock (processedlock)
                                    {
                                        ++processed_docs;
                                    }
                                }
                                else
                                {
                                    badpages++;
                                    lock (debuglock)
                                    {
                                        StreamWriter sw = new StreamWriter("badpages.txt", true);
                                        sw.WriteLine("ERROR ON PAGE");
                                        sw.WriteLine("-------------");
                                        sw.Write(Encoding.UTF8.GetString(singlepage));
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                }
                                lasttitle = title;
                                lastbody = body;
                                lastid = identifier;
                                break;
                            }
                            else
                            {
                                if (status == 1)
                                {
                                    status = 2;
                                }
                                Thread.Sleep(10);
                            }
                            if (status == 2)
                            {
                                break;
                            }
                        }
                        if (status == 2)
                        {
                            lock (lockthread)
                            {
                                activethreads--;
                                /*
                                StreamWriter sw = new StreamWriter("lastpage" + threadid + ".txt");
                                sw.WriteLine(lasttitle);
                                sw.WriteLine(lastbody);
                                sw.WriteLine(lastid);
                                sw.Close();
                                 */
                            }
                            break;
                        }
                    }
                }).Start();
            }

            //main thread is waiting for other threads to finish
            DateTime startime = DateTime.Now;
            while (1 == 1)
            {
                MemoryManager.memorystats stats = mem.GetMemStats();
                double avg = processed_docs / (DateTime.Now - startime).TotalSeconds;
                Console.Write("P: {0}, P/s: {1:#.##}, PQ:{2}, M(c/i): {3}/{4} %, R/P: {5}/{6}, of {7}, at: {8} \r", processed_docs, avg, pages.Count, Math.Round(100 * stats.usecharmem), Math.Round(100 * stats.usedshortmem), stats.reserveincidents, mem.priorityqueue, stats.overflow, activethreads);
                //Console.Write("P:{0},P/sec:{1:#.##},b:{2},EQ:{3},Q1:{4},Q2:{5},PQ:{6} \r", 
                //   processed_docs, avg, badpages, emptycontent_queue.Count, output1.Count, output2.Count, pages.Count);
                //Console.Write("P:{0},P/sec:{1:#.##},words:{2},widf:{3},concepts:{4}             \r",
                //    processed_docs, avg, tp.worddict.Count, tp.wordidf.Count, tp.conceptwords.Count);

                if (activethreads == 0)
                {
                    //raw.Close();
                    Console.WriteLine("P: {0}, P/s: {1:#.##}, PQ:{2}, M(c/i): {3}/{4} %, R/P: {5}/{6}, of {7}, at: {8}", processed_docs, avg, pages.Count, Math.Round(100 * stats.usecharmem), Math.Round(100 * stats.usedshortmem), stats.reserveincidents, mem.priorityqueue, stats.overflow, activethreads);
                    break;
                }
                Thread.Sleep(1000);
            }

            // proceed to final step
            Console.WriteLine("Finishing up ...");

            // read in stop words
            string dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            StreamReader sr = new StreamReader(dir+"/"+"stopwords.csv");
            string stopwordstr = sr.ReadToEnd();
            sr.Close();
            HashSet<int> reservedWords = new HashSet<int>();
            HashSet<int> rawstopwords = convertStopWords(stopwordstr, ref mem, ref wikidata, ref reservedWords);

            //now add concept title and categories to word dictionary
            addCatConceptWords(ref mem, ref wikidata, ref reservedWords);
            //now remove empty string
            foreach (KeyValuePair<string, int> kvp in wikidata.worddict)
            {
                if (kvp.Key.Trim() == "")
                {
                    int wordid;
                    wikidata.worddict.TryRemove(kvp.Key, out wordid);
                    if (reservedWords.Contains(wordid))
                    {
                        reservedWords.Remove(wordid);
                    }
                }
            }

            //create output files
            #region cleanwordsconcepts
            //delete words below the threshold
            HashSet<int> validwords = new HashSet<int>();
            foreach (KeyValuePair<string, int> kvp in wikidata.worddict)
            {
                if (wikidata.wordidf[kvp.Value] >= wordthreshold || reservedWords.Contains(kvp.Value))
                {
                    validwords.Add(kvp.Value);
                }
            }
            //get list of valid concepts
            HashSet<int> validconcepts = new HashSet<int>();
            foreach (KeyValuePair<int, WikiData.conceptstats> kvp in wikidata.conceptdata)
            {
                if (kvp.Value.valid)
                {
                    validconcepts.Add(kvp.Key);
                }
            }
            //get list of categories
            HashSet<int> validcats = new HashSet<int>();
            foreach (KeyValuePair<string, int> kvp in wikidata.categorydict)
            {
                validcats.Add(kvp.Value);
            }

            //now create crosswalks
            int[] dummy = validwords.ToArray<int>();
            Dictionary<int, int> crosswalk_words = new Dictionary<int, int>();
            for (int i = 0; i < dummy.Length; i++)
            {
                crosswalk_words.Add(dummy[i], i);
            }
            dummy = validconcepts.ToArray<int>();
            Dictionary<int, int> crosswalk_concepts = new Dictionary<int, int>();
            for (int i = 0; i < dummy.Length; i++)
            {
                crosswalk_concepts.Add(dummy[i], i);
            }
            dummy = validcats.ToArray<int>();
            Dictionary<int, int> crosswalk_cats = new Dictionary<int, int>();
            for (int i = 0; i < dummy.Length; i++)
            {
                crosswalk_cats.Add(dummy[i], i);
            }
            #endregion cleanwordsconcepts

            Console.WriteLine("Calculating frequency resources ...");
            createFreqResources(numthreads, wikidata, validwords, validconcepts, crosswalk_words, crosswalk_concepts,rawstopwords,bigramthreshold);

            DateTimeOffset start = DateTimeOffset.Now;            
            #region concept_outputfiles
            Console.WriteLine("Writing concept output files ...");
            //write concepts
            string filename = "concepts.txt";
            File.Delete(filename);
            StreamWriter writer = new StreamWriter(filename);
            foreach(int id in validconcepts)
            {
                writer.WriteLine(wikidata.conceptdata[id].title+"\t"+crosswalk_concepts[id]+"\t"+wikidata.conceptdata[id].conceptwords.Length);                           
            }
            writer.Close();
            //write concept titles
            filename = "concept_titles.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (int id in validconcepts)
            {
                //Console.WriteLine("{0}  {1}", id, crosswalk_concepts[id]);
                writer.Write(crosswalk_concepts[id]);
                //Console.WriteLine(wikidata.conceptdata[id].titleArray.Length);
                for (int i = 0; i < wikidata.conceptdata[id].titleArray.Length;i++)
                {
                    if (wikidata.conceptdata[id].titleArray[i] < 0)
                    {
                        writer.Write("\t" + wikidata.conceptdata[id].titleArray[i]);
                    }
                    else
                    {
                        writer.Write("\t" + crosswalk_words[wikidata.conceptdata[id].titleArray[i]]);
                    }
                }
                writer.WriteLine();
            }
            writer.Close();
            //write concept words
            filename = "concept_word.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (int id in validconcepts)
            {                
                writer.Write(crosswalk_concepts[id]);
                for (int i = 0; i < wikidata.conceptdata[id].conceptwords.Length; i++)
                {
                    int k=wikidata.conceptdata[id].conceptwords[i];
                    if (k>=0)
                    {
                        if (validwords.Contains(k))
                        {
                            k = crosswalk_words[k];
                        }
                        else
                        {
                            k = -1;
                        }
                    }
                    writer.Write("\t" + k);
                }
                writer.WriteLine();
            }
            writer.Close();
            #endregion concept_outputfiles
            Console.WriteLine("Concept files saved in {0:F2} minutes ...",(DateTimeOffset.Now - start).TotalMinutes);

            start = DateTimeOffset.Now;
            #region categories_outputfiles
            Console.WriteLine("Writing category output files ...");
            //write categories
            filename = "categories.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            Dictionary<int, string> catlist = new Dictionary<int, string>();
            //Console.WriteLine(wikidata.categorydict.Count);
            foreach (KeyValuePair<string, int> kvp in wikidata.categorydict)
            {
                catlist.Add(kvp.Value, kvp.Key);
            }
            foreach (int id in validcats)
            {
                writer.WriteLine(catlist[id]+"\t"+crosswalk_cats[id]);
            }
            writer.Close();
            //write category words
            filename = "categories_titles.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (int id in validcats)
            {
                writer.Write(crosswalk_cats[id]);
                for (int i = 0; i < wikidata.categoryTitleArray[catlist[id]].Length; i++)
                {
                    writer.Write("\t" + wikidata.categoryTitleArray[catlist[id]][i]);
                }
                writer.WriteLine();
            }
            writer.Close();
            //write category parent matrix
            filename = "categories_parentmatrix.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (KeyValuePair<string, HashSet<string>> kvp in wikidata.parentcategorydict)
            {
                if (wikidata.categorydict.ContainsKey(kvp.Key))
                {
                    writer.Write(crosswalk_cats[wikidata.categorydict[kvp.Key]]);
                }
                foreach (string cat in kvp.Value)
                {
                    if (wikidata.categorydict.ContainsKey(cat))
                    {
                        writer.Write("\t"+crosswalk_cats[wikidata.categorydict[cat]]);
                    }
                }
                writer.WriteLine();
            }
            writer.Close();
            //write concept_categories
            filename = "concept_categories.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (int id in validconcepts)
            {
                writer.Write(crosswalk_concepts[id]);
                foreach (string cat in wikidata.conceptdata[id].categories)
                {
                    if (wikidata.categorydict.ContainsKey(cat))
                    {
                        writer.Write("\t" + crosswalk_cats[wikidata.categorydict[cat]]);
                    }
                }
                writer.WriteLine();
            }            
            writer.Close();
            #endregion categories_outputfiles
            Console.WriteLine("Category files saved in {0:F2} minutes ...", (DateTimeOffset.Now - start).TotalMinutes);

            start = DateTimeOffset.Now;
            #region links_outputfiles
            Console.WriteLine("Writing links output files ...");
            //write disambiguation matrix
            filename = "disambiguation.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (KeyValuePair<string, HashSet<string>> kvp in wikidata.disambigredirect)
            {
                writer.Write(kvp.Key);
                foreach (string child in kvp.Value)
                {
                    //Console.WriteLine(child);
                    if (wikidata.conceptdict.ContainsKey(child))
                    {
                        int id2 = wikidata.conceptdict[child];
                        if (validconcepts.Contains(id2))
                        {
                            writer.Write("\t" + crosswalk_concepts[id2]);
                        }
                    }
                }
                writer.WriteLine();
            }
            writer.Close();
            //write redirects
            filename = "redirects.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (KeyValuePair<string,string> kvp in wikidata.conceptredirects)
            {
                if (kvp.Value == null)
                {
                    continue;
                }
                writer.Write(kvp.Key);
                if (wikidata.conceptdict.ContainsKey(kvp.Value))
                {
                    int id2 = wikidata.conceptdict[kvp.Value];
                    if (validconcepts.Contains(id2))
                    {
                        writer.Write("\t" + crosswalk_concepts[id2]);
                    }
                }
                writer.WriteLine();
            }
            writer.Close();
            //write concept_outlinks
            filename = "concept_outlinks.txt";
            File.Delete(filename);
            writer = new StreamWriter(filename);
            foreach (int id in validconcepts)
            {
                writer.Write(crosswalk_concepts[id]);
                foreach (string link in wikidata.conceptdata[id].outlinks)
                {
                    if (wikidata.conceptdict.ContainsKey(link))
                    {
                        int id2 = wikidata.conceptdict[link];
                        if (validconcepts.Contains(id2))
                        {
                            writer.Write("\t"+crosswalk_concepts[id2]);
                        }
                    }
                }
                writer.WriteLine();
            }
            writer.Close();
            #endregion links_outputfiles
            Console.WriteLine("Links files saved in {0:F2} minutes ...", (DateTimeOffset.Now - start).TotalMinutes);

        }

        static HashSet<int> convertStopWords(string rawstopwords, ref MemoryManager mem, ref WikiData wikidata, ref HashSet<int> reservedWords)
        {
            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            bool sticky = false;
            bool stopword = false;
            int division = 0;
            bool isInt = false;
            int decodedInt = -1;

            // create a new text processor object
            HTMLWikiProcessor hwproc = new HTMLWikiProcessor(new HashSet<int>(), false);
            DecodedTextClass dtc = new DecodedTextClass(mem, true);
            hwproc.LoadDecodedTextClass(ref dtc);
            hwproc.ProcessHTML(rawstopwords);

            // perform cleanup on the data
            HashSet<int> rawids = new HashSet<int>();

            // loop through the resulting words
            int len = dtc.NumberWords();
            int maxid=0;
            foreach(KeyValuePair<string,int> kvp in wikidata.worddict)
            {
                if (kvp.Value>maxid)
                {
                    maxid=kvp.Value;
                }
            }
            maxid++;
            for (int i = 0; i < len; i++)
            {
                if (dtc.GetWord(i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt))
                {
                    if (!isInt)
                    {
                        string token = (new string(chararray, startindex, length));                    
                        if (!wikidata.worddict.ContainsKey(token))
                        {
                        if (!wikidata.wordidf.TryAdd(maxid, 1))
                        {
                            ++wikidata.wordidf[maxid];
                        }
                            wikidata.worddict.TryAdd(token, maxid);
                            if (!wikidata.wordidf.TryAdd(maxid, 1))
                            {
                                ++wikidata.wordidf[maxid];
                            }
                            maxid++;
                        }
                        if (!reservedWords.Contains(wikidata.worddict[token]))
                        {
                            reservedWords.Add(wikidata.worddict[token]);
                        }
                        if (!rawids.Contains(wikidata.worddict[token]))
                        {
                            rawids.Add(wikidata.worddict[token]);
                        }                       
                    }
                }
            }
            return rawids;
        }

        static void addCatConceptWords(ref MemoryManager mem, ref WikiData wikidata, ref HashSet<int> reservedWords)
        {
            char[] chararray = new char[0];
            int startindex = 0;
            int length = 0;
            bool sticky = false;
            bool stopword = false;
            int division = 0;
            bool isInt = false;
            int decodedInt = -1;

            // create a new text processor object
            HTMLWikiProcessor hwproc = new HTMLWikiProcessor(new HashSet<int>(), false);
            DecodedTextClass dtc = new DecodedTextClass(mem, true);
            hwproc.LoadDecodedTextClass(ref dtc);

            int maxid = 0;
            foreach (KeyValuePair<string, int> kvp in wikidata.worddict)
            {
                if (kvp.Value > maxid)
                {
                    maxid = kvp.Value;
                }
            }
            maxid++;
            foreach (KeyValuePair<int, WikiData.conceptstats> kvp in wikidata.conceptdata)
            {
                if (!kvp.Value.valid)
                {
                    continue;
                }
                wikidata.conceptdict.Add(kvp.Value.title,kvp.Key);
                dtc.resetDecoder();
                hwproc.ProcessHTML(kvp.Value.title);
                // loop through the resulting words
                int len = dtc.NumberWords();
                int[] output = new int[len];
                for (int i = 0; i < len; i++)
                {
                    if (dtc.GetWord(i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt))
                    {
                        if (!stopword && !isInt)
                        {
                            string token = (new string(chararray, startindex, length));
                            if (!wikidata.worddict.ContainsKey(token))
                            {
                                wikidata.worddict.TryAdd(token, maxid);
                                if (!wikidata.wordidf.TryAdd(maxid, 1))
                                {
                                    ++wikidata.wordidf[maxid];
                                }
                                maxid++;
                            }
                            if (!reservedWords.Contains(wikidata.worddict[token]))
                            {
                                reservedWords.Add(wikidata.worddict[token]);
                            }
                            output[i] = wikidata.worddict[token];
                        }
                        else
                        {
                            if (isInt && decodedInt>0)
                            {
                                output[i]=-1-decodedInt;
                            }
                            else
                            {
                                output[i] = -1;
                            }
                        }
                    }
                }
                kvp.Value.titleArray = output;
            }
            foreach (KeyValuePair<string, int> kvp in wikidata.categorydict)
            {
                dtc.resetDecoder();
                hwproc.ProcessHTML(kvp.Key);
                // loop through the resulting words
                int len = dtc.NumberWords();
                int[] output = new int[len];
                for (int i = 0; i < len; i++)
                {
                    if (dtc.GetWord(i, ref chararray, ref startindex, ref length, ref sticky, ref division, ref stopword, ref isInt, ref decodedInt))
                    {
                        if (!stopword && !isInt)
                        {
                            string token = (new string(chararray, startindex, length));
                            if (!wikidata.worddict.ContainsKey(token))
                            {
                                wikidata.worddict.TryAdd(token, maxid);
                                if (!wikidata.wordidf.TryAdd(maxid, 1))
                                {
                                    ++wikidata.wordidf[maxid];
                                }
                                maxid++;
                            }
                            if (!reservedWords.Contains(wikidata.worddict[token]))
                            {
                                reservedWords.Add(wikidata.worddict[token]);
                            }
                            output[i] = wikidata.worddict[token];
                        }
                        else
                        {
                            if (isInt && decodedInt > 0)
                            {
                                output[i] = -1 - decodedInt;
                            }
                            else
                            {
                                output[i] = -1;
                            }
                        }
                    }
                }
                wikidata.categoryTitleArray.TryAdd(kvp.Key,output);
            }
        }

        static void createFreqResources(int numthreads, WikiData wikidata, HashSet<int> validwords, HashSet<int> validconcepts, Dictionary<int, int> crosswalk_words, Dictionary<int, int> crosswalk_concepts, HashSet<int> stopwords, int bigram_threshold)
        {
            //populate termfrequency class
            termfrequency freq = new termfrequency();
            Dictionary<int,string> int2word = new Dictionary<int,string>();
            foreach(KeyValuePair<string,int> kvp in wikidata.worddict)
            {
                int2word.Add(kvp.Value,kvp.Key);
            }
            foreach(int wordid in validwords)
            {
                freq.allterms.TryAdd(crosswalk_words[wordid], new termfrequency.singleterm());
                freq.allterms[crosswalk_words[wordid]].word = int2word[wordid];
            }
            //loop over all words and bigrams in all concepts            
            DateTimeOffset currentTime = DateTimeOffset.Now;
            DateTimeOffset startTime = DateTimeOffset.Now;
            int conceptcounter = 0;
            Console.WriteLine("Aggregating words and bigrams across concepts:");
            foreach (int id in validconcepts)
            {
                int mappedid = crosswalk_concepts[id];
                conceptcounter++;
                int firstbigram = -1;
                int[] stream=wikidata.conceptdata[id].conceptwords;
                for (int i = 0; i < stream.Length; i++)
                {
                    int wordid = stream[i];
                    if (stopwords.Contains(wordid) || !validwords.Contains(wordid))
                    {
                        wordid = -1;
                    }
                    else
                    {
                        if (wordid >= 0)
                        {
                            wordid = crosswalk_words[wordid];
                        }
                    }
                    //not in idf
                    if (wordid == -1)
                    {
                        firstbigram = -1;
                    }
                    else
                    {
                        if (!freq.allterms[wordid].conceptfrequency.ContainsKey(mappedid))
                        {
                            freq.allterms[wordid].conceptfrequency.Add(mappedid, 1);
                        }
                        else
                        {
                            freq.allterms[wordid].conceptfrequency[mappedid]++;
                        }
                        if (firstbigram != -1)
                        {
                            if (!freq.allterms[firstbigram].bigrams.ContainsKey(wordid))
                            {
                                freq.allterms[firstbigram].bigrams.Add(wordid, new Dictionary<int, int>());
                            }
                            if (!freq.allterms[firstbigram].bigrams[wordid].ContainsKey(mappedid))
                            {
                                freq.allterms[firstbigram].bigrams[wordid].Add(mappedid, 1);
                            }
                            else
                            {
                                freq.allterms[firstbigram].bigrams[wordid][mappedid]++;
                            }
                            firstbigram = wordid;
                        }
                        else
                        {
                            firstbigram = wordid;
                        }
                    }
                }
                if ((DateTimeOffset.Now - currentTime).TotalSeconds > 1)
                {
                    double done = 1.0 * conceptcounter / validconcepts.Count * 100;
                    Console.Write("{0:F2} percent done\r", done);
                    currentTime = DateTimeOffset.Now;
                }
            }
            Console.WriteLine("100 percent done in {0:F2} minutes.",(DateTimeOffset.Now-startTime).TotalMinutes);
            //saving data
            Console.WriteLine("Saving frequency data ...");
            startTime = DateTimeOffset.Now;
            freq.saveData(bigram_threshold);
            Console.WriteLine("Frequency data saved in {0:F2} minutes ...",(DateTimeOffset.Now - startTime).TotalMinutes);
        }

    }
}
