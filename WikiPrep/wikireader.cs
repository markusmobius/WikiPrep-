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
using System.Threading;
using Ionic.BZip2;
using Ionic.Zlib;
using System.IO;

namespace WikiPrep
{
    public struct wikistream
    {
        public string type { get; set; }
        public FileStream xmlstream { get; set; }
        public BZip2InputStream bzip2stream { get; set; }
        public GZipStream gzipstream { get; set; }
    }

    //this class takes the buffer array and reads it char by char to generate a page array
    //upon finishing reading a page, it pushes the byte[] page to the pages queue
    public class PageProcessor
    {
        byte[] pagebuffer;
        bool insidepage;
        bool insidetag;
        bool matchtag;
        bool opentag;
        int tagtextcounter;
        int endpos, currentpos;
        byte[] texttag = { (byte)'p', (byte)'a', (byte)'g', (byte)'e' };
        public PageProcessor()
        {
            insidepage = false;
            insidetag = false;
            opentag = false;
            matchtag = false;
            currentpos = 0;
            tagtextcounter = -1;
            pagebuffer = new byte[10000000];
        }
        public void AppendBuffer(byte[] buffer, int length, ref ConcurrentQueue<byte[]> pages)
        {
            for (int i = 0; i < length; i++)
            {
                if (insidepage)
                {
                    pagebuffer[currentpos] = buffer[i];
                }
                //check if tag is being opened (could be open or close tag)
                if (buffer[i] == (byte)'<')
                {
                    tagtextcounter = 0;
                    opentag = true;
                    insidetag = true;
                    matchtag = true;
                    endpos = currentpos;
                    currentpos++;
                    continue;
                }
                else
                {
                    currentpos++;
                }
                if (currentpos > 10000000)
                {
                    //exceed buffer size - discard page
                    insidepage = false;
                }
                if (insidetag && (buffer[i] == (byte)'/') && (tagtextcounter == 0))
                {
                    opentag = false; //this would be a </ closing tag
                    continue;
                }

                if (insidetag && (buffer[i] == (byte)'>') && matchtag && (tagtextcounter == 4))
                {
                    if (opentag)
                    {
                        insidepage = true;
                        insidetag = false;
                        currentpos = 0;
                    }
                    else // get here if we find </page> tag
                    {
                        if (insidepage)
                        {
                            //copy page into queue
                            if (endpos > 0)
                            {
                                byte[] singlepage = new byte[endpos];
                                Array.Copy(pagebuffer, singlepage, endpos);
                                pages.Enqueue(singlepage); //push this page to the top of the queue
                            }
                            insidepage = false; // we left the page
                        }
                    }
                }
                if (insidetag && (tagtextcounter < 4) && (texttag[tagtextcounter] == buffer[i]))
                {
                    tagtextcounter++; //if it is reading in like <page>
                }
                else
                {
                    matchtag = false;
                }
            }
        }
    }


    public static class wikireader
    {
        public static void read(wikistream wikimediastream, ref ConcurrentQueue<byte[]> pages, ref bool reader_done)
        {
            byte[] buffer = new byte[1000000];
            PageProcessor pageproc = new PageProcessor();

            while (true)
            {
                //read into buffer
                int numbytes = 0;
                switch (wikimediastream.type)
                {
                    case "xml":
                        numbytes = wikimediastream.xmlstream.Read(buffer, 0, buffer.Length);
                        break;
                    case "gz":
                        numbytes = wikimediastream.gzipstream.Read(buffer, 0, buffer.Length);
                        break;
                    case "bz2":
                        numbytes = wikimediastream.bzip2stream.Read(buffer, 0, buffer.Length);
                        break;
                }
                if (numbytes <= 0)
                {
                    break;
                }
                //now process buffer
                pageproc.AppendBuffer(buffer, numbytes, ref pages);

                //wait for other threads to catch up
                while (pages.Count > 2000)
                {
                    Thread.Sleep(100);
                }

            }
            reader_done = true;
        }
    }
}
