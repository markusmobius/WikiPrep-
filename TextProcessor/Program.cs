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
using System.Xml;

/// TO DO: add in some comments and comment on various features of the text processor

namespace TextProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamReader stream = new StreamReader(args[0]);
            string body = stream.ReadToEnd();
            MemoryManager mem = new MemoryManager(4000000, 4000000);
            DecodedTextClass content = new DecodedTextClass(mem, true);
            HTMLWikiProcessor textproc = new HTMLWikiProcessor(new HashSet<int>(), false);
            textproc.LoadDecodedTextClass(ref content);
            content.resetDecoder();
            textproc.ProcessHTML(body);

            StreamWriter sw = new StreamWriter("words.txt");
            string[] tokens = content.GetTokens();
            sw.Write(string.Join(",", tokens));
            sw.Close();

            // if text is an html page, we can extract the title
            sw = new StreamWriter("title.txt");
            tokens = content.GetTitleTokens();
            sw.Write(string.Join(",", tokens));
            sw.Close();

            // if text is an html page, we can extract text only from within div's with a matching id
            content.resetDecoder(); // need to reset to reuse the DecodedTextClass object
            HashSet<string> divfilters = new HashSet<string>();
            divfilters.Add("id=\"articleBody\"");
            divfilters.Add("class=\"articleBody\"");

            textproc.ProcessDivHTML(body, divfilters);
            sw = new StreamWriter("specificdiv.txt");
            sw.Write(string.Join(",", tokens));
            sw.Close();

        }
    }
}
