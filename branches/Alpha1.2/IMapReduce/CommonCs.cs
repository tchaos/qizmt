/**************************************************************************************
 *  MySpace’s Mapreduce Framework is a mapreduce framework for distributed computing  *
 *  and developing distributed computing applications on large clusters of servers.   *
 *                                                                                    *
 *  Copyright (C) 2008  MySpace Inc. <http://qizmt.myspace.com/>                      *
 *                                                                                    *
 *  This program is free software: you can redistribute it and/or modify              *
 *  it under the terms of the GNU General Public License as published by              *
 *  the Free Software Foundation, either version 3 of the License, or                 *
 *  (at your option) any later version.                                               *
 *                                                                                    *
 *  This program is distributed in the hope that it will be useful,                   *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of                    *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                     *
 *  GNU General Public License for more details.                                      *
 *                                                                                    *
 *  You should have received a copy of the GNU General Public License                 *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.             *
***************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{
    public class CommonCs
    {
        
        public static readonly string CommonDynamicCsCode =
    (@"

      public static int GenHash(IList<byte> data)
      {
          unchecked
          {
              int result = 0x67b97dd8 ^ data.Count;
              for(int i = 0; i < data.Count; i++)
              {
                  result += ~(result >> 30) & 0x3;
                  result += data[i];
              }
              return result;
          }
      }

          public static int GetNextInt(ByteSlice linebuf, ref int offset, int length)
          {
#if DEBUG
#else
              unchecked
#endif
              {
                  // Skip leading non-number-chars.
                  for (; ; offset++)
                  {
                      if (offset >= length)
                      {
                          return 0;
                      }
                      if ('-' == linebuf[offset]
                          || (linebuf[offset] >= '0' && linebuf[offset] <= '9'))
                      {
                          break;
                      }
                  }

                  if (offset >= length)
                  {
                      return 0;
                  }
                  bool neg = false;
                  if ('-' == linebuf[offset])
                  {
                      neg = true;
                      offset++;
                  }

                  int result = 0;
                  for (; offset < length; offset++)
                  {
                      byte by = linebuf[offset];
                      if (by >= '0' && by <= '9')
                      {
                          result *= 10;
                          result += (byte)by - '0';
                      }
                      else
                      {
                          offset++; // ...
                          break;
                      }
                  }
                  if (neg)
                  {
                      result = -result;
                  }
                  return result;
              }
          }
          public static int GetNextInt(IList<byte> linebuf, ref int offset, int length)
          {
#if DEBUG
#else
              unchecked
#endif
              {
                  // Skip leading non-number-chars.
                  for (; ; offset++)
                  {
                      if (offset >= length)
                      {
                          return 0;
                      }
                      if ('-' == linebuf[offset]
                          || (linebuf[offset] >= '0' && linebuf[offset] <= '9'))
                      {
                          break;
                      }
                  }

                  if (offset >= length)
                  {
                      return 0;
                  }
                  bool neg = false;
                  if ('-' == linebuf[offset])
                  {
                      neg = true;
                      offset++;
                  }

                  int result = 0;
                  for (; offset < length; offset++)
                  {
                      byte by = linebuf[offset];
                      if (by >= '0' && by <= '9')
                      {
                          result *= 10;
                          result += (byte)by - '0';
                      }
                      else
                      {
                          offset++; // ...
                          break;
                      }
                  }
                  if (neg)
                  {
                      result = -result;
                  }
                  return result;
              }
          }

          public static long GetNextLong(ByteSlice linebuf, ref int offset, int length)
          {
#if DEBUG
#else
              unchecked
#endif
              {
                  // Skip leading non-number-chars.
                  for (; ; offset++)
                  {
                      if (offset >= length)
                      {
                          return 0;
                      }
                      if ('-' == linebuf[offset]
                          || (linebuf[offset] >= '0' && linebuf[offset] <= '9'))
                      {
                          break;
                      }
                  }

                  if (offset >= length)
                  {
                      return 0;
                  }
                  bool neg = false;
                  if ('-' == linebuf[offset])
                  {
                      neg = true;
                      offset++;
                  }

                  long result = 0;
                  for (; offset < length; offset++)
                  {
                      byte by = linebuf[offset];
                      if (by >= '0' && by <= '9')
                      {
                          result *= 10;
                          result += (byte)by - '0';
                      }
                      else
                      {
                          offset++; // ...
                          break;
                      }
                  }
                  if (neg)
                  {
                      result = -result;
                  }
                  return result;
              }
          }
          public static long GetNextLong(IList<byte> linebuf, ref int offset, int length)
          {
#if DEBUG
#else
              unchecked
#endif
              {
                  // Skip leading non-number-chars.
                  for (; ; offset++)
                  {
                      if (offset >= length)
                      {
                          return 0;
                      }
                      if ('-' == linebuf[offset]
                          || (linebuf[offset] >= '0' && linebuf[offset] <= '9'))
                      {
                          break;
                      }
                  }

                  if (offset >= length)
                  {
                      return 0;
                  }
                  bool neg = false;
                  if ('-' == linebuf[offset])
                  {
                      neg = true;
                      offset++;
                  }

                  long result = 0;
                  for (; offset < length; offset++)
                  {
                      byte by = linebuf[offset];
                      if (by >= '0' && by <= '9')
                      {
                          result *= 10;
                          result += (byte)by - '0';
                      }
                      else
                      {
                          offset++; // ...
                          break;
                      }
                  }
                  if (neg)
                  {
                      result = -result;
                  }
                  return result;
              }
          }

        public static void PadBytes(List<byte> to, int length, byte padbyte)
        {
            for(int tc = to.Count; tc < length; tc++)
            {
                to.Add(padbyte);
            }
        }

        public static bool StreamReadAsciiLineAppend(System.IO.Stream stm, List<byte> list)
        {
            for(;;)
            {
                int ib = stm.ReadByte();
                if(-1 == ib)
                {
                    if(0 != list.Count)
                    {
                        break;
                    }
                    return false;
                }
                if((int)'\n' == ib)
                {
                    break;
                }
                if(ib >= 0x80)
                {
                    throw new System.IO.IOException(`Line from input stream is not ASCII`);
                }
                list.Add((byte)ib);
            }
            return true;
        }

          // Returns sentence, excludes it from input.
          ByteSlice ExtractSentence(ref ByteSlice input)
          {
              ByteSlice inp = input;
              
              {
                  int i = 0;
                  for(; i < inp.Length; i++)
                  {
                      byte b = inp[i];
                      if(' ' != b
                          && '\t' != b
                          && '\r' != b
                          && '\n' != b
                          && '.' != b
                          && '!' != b
                          && ',' != b
                          && ';' != b
                          && '?' != b
                          && ':' != b
                          )
                      {
                          break;
                      }
                  }
                  input = inp = ByteSlice.Prepare(inp, i, inp.Length - i);
              }
              
              {
                  bool prevend = false;
                  int i = 0;
                  ByteSlice result = ByteSlice.Prepare();
                  for(;; i++)
                  {
                      if(i == inp.Length)
                      {
                          result = inp;
                          break;
                      }
                      byte b = inp[i];
                      if(prevend)
                      {
                          if(' ' == b
                              || '\t' == b
                              || '\r' == b
                              || '\n' == b)
                          {
                              result = ByteSlice.Prepare(inp, 0, i);
                              i++; // Skip the space.
                              break;
                          }
                      }
                      else
                      {
                          prevend = false;
                          if('.' == b
                              || '!' == b
                              || '?' == b
                              || ':' == b)
                           {
                              prevend = true;
                           }
                      }
                  }
                  input = ByteSlice.Prepare(inp, i, inp.Length - i);
                  return result;
              }
          }

").Replace('`', '"');


        public static void AddCommonAssemblyReferences(List<string> dlls)
        {            
            dlls.Add("System.dll");
            dlls.Add("System.Xml.dll");
            dlls.Add("System.Data.dll");    
            dlls.Add("System.Drawing.dll");
            dlls.Add("System.Core.dll");

            dlls.Add("IMapReduce.dll");

            dlls.Add("Surrogate.dll");

            dlls.Add("LongArray.dll");
            dlls.Add("SlaveMemory.dll");
            dlls.Add("MySpace.DataMining.CollaborativeFilteringObjects.dll");
        }


        public static void AddOpenCVAssemblyReference(List<string> dlls, int bits)
        {
            const string opencvdll32 = "openCV.dll";
            const string opencvdll64 = "openCV_64.dll";
            string opencvdll;
            switch (bits)
            {
                case 32: opencvdll = opencvdll32; break;
                case 64: opencvdll = opencvdll64; break;
                default:
                    if (IntPtr.Size >= 8)
                    {
                        opencvdll = opencvdll64;
                    }
                    else
                    {
                        opencvdll = opencvdll32;
                    }
                    break;
            }
            if (null != dlls)
            {
                dlls.Add(opencvdll);
            }
            //return opencvdll;
        }

        public static void AddOpenCVAssemblyReference(List<string> dlls)
        {
            AddOpenCVAssemblyReference(dlls, 0);
        }



    }
}
