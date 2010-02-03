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
    public class StaticGlobals
    {
        public static long MapIteration = -1;        
        public static long ReduceIteration = -1;
        public static int DSpace_KeyLength = 0;
        public static int Qizmt_KeyLength
        {
            get
            {
                return DSpace_KeyLength;
            }
        }
        public static string DSpace_SlaveIP;
        public static string Qizmt_SlaveIP
        {
            get
            {
                return DSpace_SlaveIP;
            }
        }
        public static string DSpace_SlaveHost;
        public static string Qizmt_SlaveHost
        {
            get
            {
                return DSpace_SlaveHost;
            }
        }
        public static int DSpace_BlocksTotalCount;
        public static int Qizmt_BlocksTotalCount
        {
            get
            {
                return DSpace_BlocksTotalCount;
            }
        }
        public static int DSpace_BlockID;
        public static int Qizmt_BlockID
        {
            get
            {
                return DSpace_BlockID;
            }
        }
        public static ExecutionContextType ExecutionContext = ExecutionContextType.UNKNOWN;
        public static ExecutionMode ExecutionMode = ExecutionMode.RELEASE;
        public static string[] Qizmt_Hosts
        {
            get
            {
                return DSpace_Hosts;
            }
        }
        public static string[] DSpace_Hosts;
        public static string DSpace_OutputDirection = "ascending";
        public static string Qizmt_OutputDirection
        {
            get
            {
                return DSpace_OutputDirection;
            }
        }
        public static bool DSpace_OutputDirection_ascending = true;
        public static bool Qizmt_OutputDirection_ascending
        {
            get
            {
                return DSpace_OutputDirection_ascending;
            }
        }
        public static int DSpace_InputRecordLength = int.MinValue;
        public static int Qizmt_InputRecordLength
        {
            get
            {
                return DSpace_InputRecordLength;
            }
        }
        public static int DSpace_OutputRecordLength = int.MinValue;
        public static int Qizmt_OutputRecordLength
        {
            get
            {
                return DSpace_OutputRecordLength;
            }
        }
        public static int DSpace_MaxDGlobals = 0;
        public static int Qizmt_MaxDGlobals
        {
            get
            {
                return DSpace_MaxDGlobals;
            }
        }
        public static bool DSpace_Last = false;
        public static bool Qizmt_Last
        {
            get
            {
                return DSpace_Last;
            }
        }
        public static long DSpace_InputBytesRemain = Int64.MaxValue;
        public static string DSpace_InputFileName = null;
        public static string Qizmt_InputFileName
        {
            get
            {
                return DSpace_InputFileName;
            }
        }
        public static long Qizmt_InputBytesRemain
        {
            get
            {
                return DSpace_InputBytesRemain;
            }
        }
    }

    public enum ExecutionContextType
    {
        UNKNOWN,
        MAP,
        LOCAL,
        REDUCE,
        REMOTE
        //REDUCEINITIALIZE,
        //REDUCEFINALIZE
    }

    public enum ExecutionMode
    {
        RELEASE,
        DEBUG
    }
}
