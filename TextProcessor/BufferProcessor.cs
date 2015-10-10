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
    //the buffer Processor keeps the last few characters in a rolling buffer
    public class BufferProcessor
    {
        char[] rollingbuffer;
        int pos;
        int bufferlength;
        public BufferProcessor(int size)
        {
            rollingbuffer = new char[size];
            bufferlength = size;
        }
        public void initialize()
        {
            pos = 0;
            for (int i = 0; i < bufferlength; i++)
            {
                rollingbuffer[i] = ' ';
            }
        }
        public void AddLetter(char letter)
        {
            rollingbuffer[pos] = letter;
            pos++;
            if (pos >= bufferlength)
            {
                pos = 0;
            }
        }
        public char GetPastLetter(int offset)
        {
            int newpos = pos + offset;
            if (newpos < 0)
            {
                return rollingbuffer[newpos + bufferlength];
            }
            return rollingbuffer[newpos];
        }
    }
}
