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
using System.Collections.Concurrent;
using System.Threading;

namespace TextProcessor
{
    public class MemoryManager
    {
        char[] globalcharacters;
        ushort[] globalshorts;
        ConcurrentQueue<int> freechunks_characters;
        ConcurrentQueue<int> freechunks_shorts;
        ConcurrentQueue<int> reservedchunks_characters;
        ConcurrentQueue<int> reservedchunks_shorts;

        int totalchunks_characters;
        int totalchunks_shorts;
        int thresholdchunk_characters;
        int thresholdchunk_shorts;
        //chunk parameters
        int chunksize_characters = 5000;
        int chunksize_shorts = 1000;
        public double reservedshare = 0.25;
        //memory map
        class singleTPmemorymap
        {
            //general state
            public bool full { get; set; }

            //charmap
            public int[] charmap { get; set; }
            public int charmap_segments { get; set; }
            public int charmap_offset { get; set; }

            //wordstore: offset, length, division (int); sticky, stopword(bool - but stored jointly in a short integer)
            public int[] intmap_words { get; set; }
            public int intmap_words_segments { get; set; }
            public int wordcounter { get; set; }
            public int nonstopwordcounter;

            //wikistore: offset, length, types (int)
            public int[] intmap_wiki { get; set; }
            public int intmap_wiki_segments { get; set; }
            public int wikicounter { get; set; }

            //dummy variables
            public char[] dummychars;
            public ushort[] dummyshort;

            public singleTPmemorymap(int maxcharchunks, int maxintchunks)
            {
                //Console.WriteLine(maxcharchunks);
                full = false;
                charmap = new int[maxcharchunks];
                charmap_offset = 0;
                charmap_segments = 0;
                intmap_words = new int[maxintchunks];
                wordcounter = 0;
                nonstopwordcounter = 0;
                intmap_words_segments = 0;

                intmap_wiki = new int[maxintchunks];
                wikicounter = 0;
                intmap_wiki_segments = 0;

                dummychars = new char[2000];
                dummyshort = new ushort[20];
            }

            public void Emptymap()
            {
                full = false;
                charmap_offset = 0;
                charmap_segments = 0;
                wordcounter = 0;
                nonstopwordcounter = 0;
                intmap_words_segments = 0;
                wikicounter = 0;
                intmap_wiki_segments = 0;

            }

        }
        ConcurrentDictionary<int, singleTPmemorymap> TPmemorymap;
        Object memlock, initmemlock, releaselock, net35lock;
        public int priorityqueue;
        ConcurrentDictionary<int, bool> TPrunning;

        //statistics
        public struct memorystats
        {
            public double usecharmem { get; set; }
            public double usedshortmem { get; set; }
            public int reserveincidents { get; set; }
            public int overflow { get; set; }
        }
        memorystats memstats;

        public MemoryManager(int totalcharsize, int totalshortsize)
        {
            globalcharacters = new char[totalcharsize];
            globalshorts = new ushort[totalshortsize];
            totalchunks_characters = totalcharsize / chunksize_characters;
            totalchunks_shorts = totalshortsize / chunksize_shorts;
            thresholdchunk_characters = (int)((1 - reservedshare) * totalchunks_characters);
            thresholdchunk_shorts = (int)((1 - reservedshare) * totalchunks_shorts);
            //fill queues
            freechunks_characters = new ConcurrentQueue<int>();
            reservedchunks_characters = new ConcurrentQueue<int>();
            for (int i = 0; i < thresholdchunk_characters; i++)
            {
                freechunks_characters.Enqueue(i);
            }
            for (int i = thresholdchunk_characters; i < totalchunks_characters; i++)
            {
                reservedchunks_characters.Enqueue(i);
            }
            freechunks_shorts = new ConcurrentQueue<int>();
            reservedchunks_shorts = new ConcurrentQueue<int>();
            for (int i = 0; i < thresholdchunk_shorts; i++)
            {
                freechunks_shorts.Enqueue(i);
            }
            for (int i = thresholdchunk_shorts; i < totalchunks_shorts; i++)
            {
                reservedchunks_shorts.Enqueue(i);
            }

            //declare memory map
            TPmemorymap = new ConcurrentDictionary<int, singleTPmemorymap>();
            TPrunning = new ConcurrentDictionary<int, bool>();
            memlock = new object();
            initmemlock = new object();
            releaselock = new object();
            net35lock = new object();
            priorityqueue = -1;
            memstats = new memorystats();
            memstats.overflow = 0;
            memstats.reserveincidents = 0;
        }

        public int InitializeMemory()
        {
            int maxcharchunks = (int)(reservedshare * globalcharacters.Length / chunksize_characters);
            int maxintchunks = (int)(reservedshare * globalshorts.Length / chunksize_shorts);
            int threadid = 0;
            lock (initmemlock)
            {
                threadid = TPmemorymap.Count;
                TPmemorymap.TryAdd(threadid, new singleTPmemorymap(maxcharchunks, maxintchunks));
                TPrunning.TryAdd(threadid, false);
            }
            return threadid;
        }

        public void ReleaseMemory(int threadid)
        {
            for (int i = 0; i < TPmemorymap[threadid].charmap_segments; i++)
            {
                int memseg = TPmemorymap[threadid].charmap[i];
                if (memseg < thresholdchunk_characters)
                {
                    freechunks_characters.Enqueue(memseg);
                }
                else
                {
                    reservedchunks_characters.Enqueue(memseg);
                }
            }
            for (int i = 0; i < TPmemorymap[threadid].intmap_words_segments; i++)
            {
                int memseg = TPmemorymap[threadid].intmap_words[i];
                if (memseg < thresholdchunk_shorts)
                {
                    freechunks_shorts.Enqueue(memseg);
                }
                else
                {
                    reservedchunks_shorts.Enqueue(memseg);
                }
            }
            for (int i = 0; i < TPmemorymap[threadid].intmap_wiki_segments; i++)
            {
                int memseg = TPmemorymap[threadid].intmap_wiki[i];
                if (memseg < thresholdchunk_shorts)
                {
                    freechunks_shorts.Enqueue(memseg);
                }
                else
                {
                    reservedchunks_shorts.Enqueue(memseg);
                }
            }
            TPmemorymap[threadid].Emptymap();
            lock (releaselock)
            {
                TPrunning[threadid] = false;
                if (threadid == priorityqueue)
                {
                    priorityqueue--;
                }
            }
        }

        public int DequeueMemory(int threadid, bool charmode)
        {
            int newseg = -1;
            TPrunning[threadid] = true;
            while (1 == 1)
            {
                lock (memlock)
                {
                    //if there is memory pressure and it's not the thread turn to finish its computations wait
                    if (priorityqueue != -1 && threadid != priorityqueue)
                    {
                        //check if priorityqueue is stuck at thread that doesn't process anymore
                        while (priorityqueue != -1 && !TPrunning[priorityqueue])
                        {
                            priorityqueue--;
                        }
                        Thread.Sleep(10);
                        continue;
                    }
                    bool success = false;
                    if (charmode)
                    {
                        success = freechunks_characters.TryDequeue(out newseg);
                    }
                    else
                    {
                        success = freechunks_shorts.TryDequeue(out newseg);
                    }
                    if (!success)
                    {
                        if (priorityqueue == -1)
                        {
                            //queue of free chunks is empty - only allow threads to finish their current computations
                            priorityqueue = TPmemorymap.Count - 1;
                            memstats.reserveincidents++;
                        }
                        if (threadid == priorityqueue)
                        {
                            //dequeue from reserved chunks
                            bool rsuccess = false;
                            if (charmode)
                            {
                                rsuccess = reservedchunks_characters.TryDequeue(out newseg);
                            }
                            else
                            {
                                rsuccess = reservedchunks_shorts.TryDequeue(out newseg);
                            }
                            if (rsuccess)
                            {
                                break;
                            }
                            else
                            {
                                newseg = -1;
                                break;
                            }
                        }
                        else
                        {
                            Thread.Sleep(10);
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
            return newseg;
        }


        public int LongestThread()
        {
            int[] threadids = TPmemorymap.Keys.ToArray<int>();
            int longestid = -1;
            int wordcounter = -1;
            foreach (int threadid in threadids)
            {
                if (TPmemorymap[threadid].wordcounter > wordcounter)
                {
                    longestid = threadid;
                }
            }
            return longestid;
        }

        public bool AddWord(int threadid, char[] chararray, int chararraylength, bool sticky, int division, bool stopword, bool isInt, int decodedInt)
        {
            singleTPmemorymap map = TPmemorymap[threadid];
            if (map.full) { return false; }
            if (isInt)
            {
                //Console.WriteLine(decodedInt);
                if (AddCharacters(threadid, chararray, 0, 0) && AddShortsWords(threadid, decodedInt, 0, division, sticky, stopword, true))
                {
                    map.charmap_offset += 0;
                    map.wordcounter++;
                    ++map.nonstopwordcounter;
                }
                else
                {
                    memstats.overflow++;
                }
            }
            else
            {
                if (AddCharacters(threadid, chararray, 0, chararraylength) && AddShortsWords(threadid, TPmemorymap[threadid].charmap_offset, chararraylength, division, sticky, stopword, false))
                {
                    map.charmap_offset += chararraylength;
                    map.wordcounter++;

                    if (!stopword)
                    {
                        ++map.nonstopwordcounter;
                    }
                }
                else
                {
                    memstats.overflow++;
                }
            }
            return true;
        }

        public bool AddCharacters(int threadid, char[] chararray, int coffset, int chararraylength)
        {
            //Console.Write(new string(chararray,coffset,chararraylength));
            //Console.Write(" ");
            singleTPmemorymap map = TPmemorymap[threadid];
            //make space for required number of characters
            int newoffset = map.charmap_offset + chararraylength;
            int max = newoffset / chunksize_characters + 1;
            if (max > map.charmap_segments)
            {
                for (int i = 0; i < (max - map.charmap_segments); i++)
                {
                    int newseg = DequeueMemory(threadid, true);
                    if (newseg == -1)
                    {
                        map.full = true;
                        return false;
                    }
                    map.charmap[map.charmap_segments + i] = newseg;
                }
                map.charmap_segments = max;
            }
            bool done_copying = false;
            int internaloffset = map.charmap_offset % chunksize_characters;
            int currentseg = map.charmap_offset / chunksize_characters;
            int remaininglength = chararraylength;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_characters)
                {
                    Array.Copy(chararray, coffset, globalcharacters, map.charmap[currentseg] * chunksize_characters + internaloffset, chunksize_characters - internaloffset);
                    coffset += chunksize_characters - internaloffset;
                    remaininglength -= chunksize_characters - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    Array.Copy(chararray, coffset, globalcharacters, map.charmap[currentseg] * chunksize_characters + internaloffset, remaininglength);
                    done_copying = true;
                }
            }
            //Console.WriteLine(new string(globalcharacters,0,5000));
            return true;
        }

        //5 short integers to code the word integers
        public bool AddShortsWords(int threadid, int offset, int length, int division, bool sticky, bool stopword, bool isInt)
        {
            singleTPmemorymap map = TPmemorymap[threadid];
            //make space for required number of characters
            int newoffset = map.wordcounter * 5 + 5;
            int max = newoffset / chunksize_shorts + 1;
            if (max > map.intmap_words_segments)
            {
                for (int i = 0; i < (max - map.intmap_words_segments); i++)
                {
                    int newseg = DequeueMemory(threadid, false);
                    if (newseg == -1)
                    {
                        map.full = true;
                        return false;
                    }
                    if (map.intmap_words_segments + i < map.intmap_words.Length)
                    {
                        map.intmap_words[map.intmap_words_segments + i] = newseg;
                    }
                    else
                    {
                        map.full = true;
                        return false;
                    }
                }
                map.intmap_words_segments = max;
            }
            //fill dummy short
            map.dummyshort[0] = (ushort)(offset >> 16);
            map.dummyshort[1] = (ushort)(offset % (2 << 16));
            map.dummyshort[2] = (ushort)(length >> 16);
            map.dummyshort[3] = (ushort)(length % (2 << 16));
            int combined = division % 256;
            if (sticky)
            {
                combined = combined | (1 << 8);
            }
            if (stopword)
            {
                combined = combined | (1 << 9);
            }
            if (isInt)
            {
                combined = combined | (1 << 10);
            }
            map.dummyshort[4] = (ushort)combined;
            //now copy dummyshort
            bool done_copying = false;
            int internaloffset = (map.wordcounter * 5) % chunksize_shorts;
            int currentseg = (map.wordcounter * 5) / chunksize_shorts;
            int coffset = 0;
            int remaininglength = 5;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_shorts)
                {
                    Array.Copy(map.dummyshort, coffset, globalshorts, map.intmap_words[currentseg] * chunksize_shorts + internaloffset, chunksize_shorts - internaloffset);
                    coffset += chunksize_shorts - internaloffset;
                    remaininglength -= chunksize_shorts - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    Array.Copy(map.dummyshort, coffset, globalshorts, map.intmap_words[currentseg] * chunksize_shorts + internaloffset, remaininglength);
                    done_copying = true;
                }
            }
            return true;
        }

        public bool AddWikiConstruct(int threadid, char[] chararray, int offset, int chararraylength, int wikitype)
        {
            //TPmemorymap = new ConcurrentDictionary<int, singleTPmemorymap>(); is instantiated in MemoryManager constructor
            singleTPmemorymap map = TPmemorymap[threadid];
            if (map.full) { return false; }
            if (AddCharacters(threadid, chararray, offset, chararraylength) && AddShortsWiki(threadid, map.charmap_offset, chararraylength, wikitype))
            {
                map.charmap_offset += chararraylength;
                map.wikicounter++;
            }
            else
            {
                memstats.overflow++;
            }
            return true;
        }

        //5 short integers to code the wiki construct
        public bool AddShortsWiki(int threadid, int offset, int length, int type)
        {
            singleTPmemorymap map = TPmemorymap[threadid];
            //make space for required number of characters
            int newoffset = map.wikicounter * 5 + 5;
            int max = newoffset / chunksize_shorts + 1;
            if (max > map.intmap_wiki_segments)
            {
                for (int i = 0; i < (max - map.intmap_wiki_segments); i++)
                {
                    int newseg = DequeueMemory(threadid, false);
                    if (newseg == -1)
                    {
                        map.full = true;
                        return false;
                    }
                    if (map.intmap_wiki_segments + i < map.intmap_wiki.Length)
                    {
                        map.intmap_wiki[map.intmap_wiki_segments + i] = newseg;
                    }
                    else
                    {
                        map.full = true;
                        return false;
                    }
                }
                map.intmap_wiki_segments = max;
            }
            //fill dummy short
            map.dummyshort[0] = (ushort)(offset >> 16);
            map.dummyshort[1] = (ushort)(offset % (2 << 16));
            map.dummyshort[2] = (ushort)(length >> 16);
            map.dummyshort[3] = (ushort)(length % (2 << 16));
            map.dummyshort[4] = (ushort)type;
            //now copy dummyshort
            bool done_copying = false;
            int internaloffset = (map.wikicounter * 5) % chunksize_shorts;
            int currentseg = (map.wikicounter * 5) / chunksize_shorts;
            //Console.Write(map.intmap_wiki[currentseg]);
            //Console.Write(" ");
            int coffset = 0;
            int remaininglength = 5;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_shorts)
                {
                    Array.Copy(map.dummyshort, coffset, globalshorts, map.intmap_wiki[currentseg] * chunksize_shorts + internaloffset, chunksize_shorts - internaloffset);
                    coffset += chunksize_shorts - internaloffset;
                    remaininglength -= chunksize_shorts - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    Array.Copy(map.dummyshort, coffset, globalshorts, map.intmap_wiki[currentseg] * chunksize_shorts + internaloffset, remaininglength);
                    done_copying = true;
                }
            }
            return true;
        }

        public int NumberWords(int threadid)
        {
            return TPmemorymap[threadid].wordcounter;
        }
        public int NumberGoodWords(int threadid)
        {
            return TPmemorymap[threadid].nonstopwordcounter;
        }

        public bool GetWord(int threadid, int i, ref char[] chararray, ref int startindex, ref int length, ref bool sticky, ref int division, ref bool stopword, ref bool isInt, ref int decodedInt)
        {
            singleTPmemorymap map = TPmemorymap[threadid];
            //copy shorts into dummyshort array
            bool done_copying = false;
            int internaloffset = i * 5 % chunksize_shorts;
            int currentseg = i * 5 / chunksize_shorts;
            int coffset = 0;
            int remaininglength = 5;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_shorts)
                {
                    Array.Copy(globalshorts, map.intmap_words[currentseg] * chunksize_shorts + internaloffset, map.dummyshort, coffset, chunksize_shorts - internaloffset);
                    coffset += chunksize_shorts - internaloffset;
                    remaininglength -= chunksize_shorts - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    Array.Copy(globalshorts, map.intmap_words[currentseg] * chunksize_shorts + internaloffset, map.dummyshort, coffset, remaininglength);
                    done_copying = true;
                }
            }
            //Console.WriteLine("{0} {1} {2} {3} {4}", map.dummyshort[0], map.dummyshort[1], map.dummyshort[2], map.dummyshort[3], map.dummyshort[4]);
            //extract information from dummyshort
            int offset = (((int)map.dummyshort[0]) << 16) + (int)map.dummyshort[1];
            length = (((int)map.dummyshort[2]) << 16) + (int)map.dummyshort[3];
            //Console.WriteLine(length);
            division = map.dummyshort[4] % 256;
            if ((map.dummyshort[4] >> 8) % 2 == 1)
            {
                sticky = true;
            }
            else
            {
                sticky = false;
            }
            if ((map.dummyshort[4] >> 9) % 2 == 1)
            {
                stopword = true;
            }
            else
            {
                stopword = false;
            }
            if ((map.dummyshort[4] >> 10) % 2 == 1)
            {
                isInt = true;
                //Console.WriteLine(length);
            }
            else
            {
                isInt = false;
            }
            if (isInt)
            {
                decodedInt = offset;
                //Console.WriteLine(decodedInt);
            }
            else
            {
                GetChars(threadid, offset, length, ref chararray, ref startindex);
            }
            return true;
        }

        public void GetChars(int threadid, int offset, int length, ref char[] chararray, ref int startindex)
        {
            //Console.WriteLine(offset);
            //Console.WriteLine(new string(globalcharacters, 0, 5000));
            singleTPmemorymap map = TPmemorymap[threadid];
            //get array
            bool done_copying = false;
            int internaloffset = offset % chunksize_characters;
            int currentseg = offset / chunksize_characters;
            int remaininglength = length;
            if (internaloffset + remaininglength <= chunksize_characters)
            {
                //character extract does not exceeds chunk boundary
                chararray = globalcharacters;
                startindex = map.charmap[currentseg] * chunksize_characters + internaloffset;
                //Console.Write(new string(globalcharacters, startindex, length));
                //Console.Write(" ");
                return;
            }
            chararray = map.dummychars;
            startindex = 0;
            int coffset = 0;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_characters)
                {
                    //character extract exceeds chunk boundary - need to copy everything to dummy
                    Array.Copy(globalcharacters, map.charmap[currentseg] * chunksize_characters + internaloffset, chararray, coffset, chunksize_characters - internaloffset);
                    coffset += chunksize_characters - internaloffset;
                    remaininglength -= chunksize_characters - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    Array.Copy(globalcharacters, map.charmap[currentseg] * chunksize_characters + internaloffset, chararray, coffset, remaininglength);
                    done_copying = true;
                }
            }
        }

        public int NumberWikiConstructs(int threadid)
        {
            return TPmemorymap[threadid].wikicounter;
        }

        public bool GetWikiConstruct(int threadid, int i, ref int wikitype, ref char[] wikiarray, ref int startindex, ref int length)
        {
            singleTPmemorymap map = TPmemorymap[threadid];
            //copy shorts into dummyshort array
            bool done_copying = false;
            int internaloffset = i * 5 % chunksize_shorts;
            int currentseg = i * 5 / chunksize_shorts;
            int coffset = 0;
            int remaininglength = 5;
            while (!done_copying)
            {
                if (internaloffset + remaininglength > chunksize_shorts)
                {
                    //Console.WriteLine(" break ");
                    Array.Copy(globalshorts, map.intmap_wiki[currentseg] * chunksize_shorts + internaloffset, map.dummyshort, coffset, chunksize_shorts - internaloffset);
                    coffset += chunksize_shorts - internaloffset;
                    remaininglength -= chunksize_shorts - internaloffset;
                    internaloffset = 0;
                    currentseg++;
                }
                else
                {
                    //Console.WriteLine(" nobreak ");
                    Array.Copy(globalshorts, map.intmap_wiki[currentseg] * chunksize_shorts + internaloffset, map.dummyshort, coffset, remaininglength);
                    done_copying = true;
                }
            }
            //extract information from dummyshort
            int offset = (((int)map.dummyshort[0]) << 16) + (int)map.dummyshort[1];
            length = (((int)map.dummyshort[2]) << 16) + (int)map.dummyshort[3];
            wikitype = map.dummyshort[4];
            GetChars(threadid, offset, length, ref wikiarray, ref startindex);
            //Console.Write(offset);
            //Console.Write(" {0} {1} {2} ",startindex,length,wikiarray.ToString());
            //Console.Write(new string(wikiarray, startindex, length));
            //Console.Write(" ");
            return true;
        }

        public memorystats GetMemStats()
        {
            memstats.usecharmem = 1.0 * ((int)((1 - reservedshare) * totalchunks_characters) - freechunks_characters.Count) / totalchunks_characters;
            memstats.usedshortmem = 1.0 * ((int)((1 - reservedshare) * totalchunks_shorts) - freechunks_shorts.Count) / totalchunks_shorts;
            return memstats;
        }
    }
}
