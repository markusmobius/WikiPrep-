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
using System.IO;
using System.Collections.Concurrent;

namespace WikiPrep
{
    public class termfrequency
    {
        public class singleterm
        {
            public Dictionary<int, int> conceptfrequency { get; set; }
            public string word { get; set; }
            public Dictionary<int, Dictionary<int, int>> bigrams { get; set; }
            public singleterm()
            {
                conceptfrequency = new Dictionary<int, int>();
                bigrams = new Dictionary<int, Dictionary<int, int>>();
            }
        }
        public ConcurrentDictionary<int, singleterm> allterms;

        public termfrequency() 
        {
            allterms = new ConcurrentDictionary<int, singleterm>();
        }

        public void saveData(int bigram_threshold)
        {
            string wordfile = "words.txt";
            File.Delete(wordfile);
            string filename = "wordfrequency.txt";
            File.Delete(filename);
            string filename_details = "wordfrequency_details.txt";
            File.Delete(filename_details);
            StreamWriter sr_words = new StreamWriter(wordfile);
            StreamWriter sr = new StreamWriter(filename);
            StreamWriter sr_details = new StreamWriter(filename_details);
            int[] keys = allterms.Keys.ToArray<int>();
            Array.Sort(keys);
            long filteredbigrams = 0;
            foreach(int key in keys)
            {
                sr_words.WriteLine(allterms[key].word + "\t" + key + "\t" + allterms[key].conceptfrequency.Count);
                sr.Write(allterms[key].word + "\t" + key + "\t" + allterms[key].conceptfrequency.Count);
                sr_details.Write(allterms[key].word + "\t" + key + "\t" + allterms[key].conceptfrequency.Count);
                foreach (KeyValuePair<int, int> kvp2 in allterms[key].conceptfrequency)
                {
                    sr_details.Write("\t"+kvp2.Key+"\t"+kvp2.Value);
                }
                //now add bigrams
                //first fine bigrams above the threshold
                HashSet<int> allowed = new HashSet<int>();
                foreach (KeyValuePair<int, Dictionary<int, int>> kvp2 in allterms[key].bigrams)
                {
                    if (kvp2.Value.Count >= bigram_threshold)
                    {
                        allowed.Add(kvp2.Key);
                    }
                }
                filteredbigrams += allterms[key].bigrams.Count - allowed.Count;
                sr.Write("\t" + allowed.Count);
                sr_details.Write("\t" + allowed.Count);
                foreach (int secondword in allowed)
                {
                    sr.Write("\t" + allterms[secondword].word+"\t"+secondword+"\t"+allterms[key].bigrams[secondword].Count);
                    sr_details.Write("\t" + secondword + "\t" + allterms[key].bigrams[secondword].Count);
                    foreach (KeyValuePair<int, int> kvp3 in allterms[key].bigrams[secondword])
                    {
                        sr_details.Write("\t" + kvp3.Key + "\t" + kvp3.Value);
                    }
                }
                sr.WriteLine();
                sr_details.WriteLine();
            }
            sr.Close();
            sr_details.Close();
            sr_words.Close();
            Console.WriteLine("{0} bigrams were filtered out",filteredbigrams);
        }
    }
}
