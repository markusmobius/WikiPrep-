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
using TextProcessor;

namespace WikiPrep
{
    //this class extracts title, identifier and text from a single wikimedia XML page
    public static class PageExtractor
    {
        //title is article title
        //id is unique int id for each article
        //in our dataset, only one revision (most recent) per page, identified by unique int id
        //text is the body text of the most recent revision
        //redirect is provided title of article to redirect to as in <redirect title="Amoeboid" />
        //redirect also given in body text as in <text xml:space="preserve">#REDIRECT [[Amoeboid]] {{R from CamelCase}}</text>
        //so we can grab the redirect from the second rather than storing additional data
        public static string[] tags = { "title", "id", "revision", "text", "redirect" };

        public static void GetPage(byte[] page, ref string body, ref string identifier, ref string title, ref bool redirect, ref string redirecttitle)
        {
            //string opage = Encoding.UTF8.GetString(page, 0, page.Length);
            body = "";
            identifier = "";
            title = "";
            redirect = false;
            redirecttitle = "";
            //parse through entire text
            int[] pipeline = new int[10]; //holds pipeline of tags we've read without seeing a closing
            int depth = 0; //measures how many open tags we've passed that haven't been closed
            int tagtype = -1;
            int tagdiff = 0;
            int stringstart = 0;
            int stringend = 0;
            int correction = 0;
            Dictionary<string, string> extrataginfo = new Dictionary<string, string>();
            for (int i = 0; i < page.Length; i++)
            {
                if (page[i] == (byte)'<') //if we find a tag being opened
                {
                    //stringend is set to the opening < of the tag we are reading
                    stringend = i;
                    //i is set to the index of the > on the tag we just read - zip to the end of the tag
                    i = getTag(page, i, ref tagdiff, ref tagtype,4,ref extrataginfo);
                    if (tagdiff < 0) // this would mean we just found the closing tag
                    {
                        //check whether certain conditions are met
                        //check which tag we are on and how it is nested, and if it matches structurally
                        //with one of our refs, update the ref to be accessed in Main() from where this was called
                        if (pipeline[0] == 0 && depth == 1)
                        {
                            title = Encoding.UTF8.GetString(page, 0, stringend - stringstart - correction);
                        }
                        if (pipeline[0] == 1 && depth == 1)
                        {
                            identifier = Encoding.UTF8.GetString(page, 0, stringend - stringstart - correction);
                        }
                        if (pipeline[0] == 2 && pipeline[1] == 3 && depth == 2)
                        {
                            body = Encoding.UTF8.GetString(page, 0, stringend - stringstart - correction);
                        }
                        if (depth > 0) { depth--; }
                    }
                    if (tagdiff == 0 & tagtype == 4) //tag like <...../> and "redirect"
                    {
                        //foreach (KeyValuePair<string, string> kvp in extrataginfo)
                        //{
                        //    Console.WriteLine(kvp.Value);
                        //}
                        if (extrataginfo.ContainsKey("title"))
                        {
                            redirect = true;
                            redirecttitle = extrataginfo["title"];
                        }
                        extrataginfo = new Dictionary<string, string>();
                    }
                    if (tagdiff > 0) //opening tag <...>
                    {
                        if (depth < pipeline.Length)
                        {
                            pipeline[depth] = tagtype; //store the tagtype in the pipeline of opened but ~ yet closed tags
                        }
                        stringstart = i + 1; //set stringstart to the first byte after the > of the opening tag
                        correction = 0;
                        depth++; //going deeeeeeper (to the next tag perhaps in our pipeline)
                    }
                }
                else //we haven't found a tag yet, we're either in content or just plain lost
                {
                    //we are outside a tag
                    //copy everything to beginning of page
                    //Console.WriteLine("{0} {1} {2}", i, stringstart, correction);
                    page[i - stringstart - correction] = page[i];
                    if (page[i] == (byte)';') // we find a semicolon in the body
                    {
                        if (i - stringstart - correction > 2) // and we're at least 2 characters into this string
                        {
                            //xml can't have < or > in the body, so we have to check if we have an encoded version (&lt; or &g;t), switch it to the < or >, and increment correction because we have
                            //shifted four bytes to a single one (true for second if as well)
                            if (page[i - stringstart - correction - 3] == (byte)'&' && page[i - stringstart - correction - 2] == (byte)'l' && page[i - stringstart - correction - 1] == (byte)'t')
                            {
                                page[i - stringstart - correction - 3] = (byte)'<';
                                correction += 3;
                            }
                            else
                            {
                                if (page[i - stringstart - correction - 3] == (byte)'&' && page[i - stringstart - correction - 2] == (byte)'g' && page[i - stringstart - correction - 1] == (byte)'t')
                                {
                                    page[i - stringstart - correction - 3] = (byte)'>';
                                    correction += 3;
                                }
                            }
                        }
                    }
                }
            }
        }

        public static int getTag(byte[] page, int offset, ref int tagdiff, ref int tagtype, int getextrainfo_fromtagtype, ref Dictionary<string,string> extrataginfo)
        {
            //^^^offset is where the tag began in the page byte array
            //set tagtype ref to default of -1 (not possible index), since no tag match yet
            tagtype = -1;
            //look for closing tag
            for (int i = offset + 1; i < page.Length; i++) // start at next byte after <
            {
                if (page[i] == (byte)'>') //if we reach the end of the tag
                {
                    //now check tagtype
                    for (int t = 0; t < tags.Length; t++) //go through all of the tag types
                    {
                        bool match = true;
                        int j = 0;
                        for (j = 0; j < tags[t].Length; j++) //read in this tag by char
                        {
                            if (offset + 1 + j > i) //if this given tag is longer than the tag we
                                //just read in from the tags array, then clearly it isnt a match
                            {
                                match = false;
                                break;
                            }
                            //if the byte of the tag were checking doesnt match the tag from the tags array
                            if (page[offset + 1 + j] != (byte)tags[t][j])
                            {
                                match = false;
                                break;
                            }
                        }
                        //if we made it through without breaking and setting match to false, we must have a real match
                        if (match)
                        {
                            tagtype = t;
                            if (tagtype == getextrainfo_fromtagtype)
                            {
                                if (i - offset - 1 - j - 1 > 0)
                                {
                                    char[] extra = new char[i - offset - 1 - j - 1];
                                    for (int k = 0; k < extra.Length; k++)
                                    {
                                        extra[k] = (char)page[offset + 1 + j + k];
                                    }
                                    string extrastring = new string(extra);
                                    string[] dummy = extrastring.Split('=');
                                    for (int k = 0; k < dummy.Length / 2; k++)
                                    {
                                        string var = dummy[2 * k].Replace('"', ' ').Trim();
                                        string val = dummy[2 * k + 1].Replace('"', ' ').Trim();
                                        if (!extrataginfo.ContainsKey(var))
                                        {
                                            extrataginfo.Add(var, val);
                                        }
                                        //Console.WriteLine("extra tag info -- {0} {1}",var,val);
                                    }
                                }
                            }
                            break;
                        }
                    }
                    //check whether tag is open or closing
                    //opening = 1, closing = -1, open/close same tag = 0
                    tagdiff = 1;
                    if (page[offset + 1] == (byte)'/')
                    {
                        tagdiff = -1;
                    }
                    //this would be a <.../> tag with no content (except for within the tag itself)
                    //this tag type closes itself and so does not increase tag closing count
                    if (page[i - 1] == (byte)'/')
                    {
                        tagdiff = 0;
                    }
                    //return the index of the closing > of the tag, along with modifying the refs with 
                    //tagdiff (to check number of opened and closed tags) and tagtype=index of the tag array corresponding to this tag type
                    return i;
                }
            }
            //if we never get a match we must have reached the end of the document without finding another tag
            //so just reuturn the final index
            tagdiff = 0;
            return page.Length;
        }
    }
}
