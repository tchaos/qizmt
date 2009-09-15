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

namespace MySpace.DataMining.AELight
{
    public partial class AELight
    {
        static int _isquiet = -1;

        public static bool QuietMode
        {
            get
            {
                if (-1 == _isquiet)
                {
                    _isquiet = 1;
                    if (null == Environment.GetEnvironmentVariable("DOSLAVE"))
                    {
                        _isquiet = 0;
                    }
                }
                return _isquiet != 0;
            }
        }


        public static string GetFriendlyByteSize(long size)
        {
            return Surrogate.GetFriendlyByteSize(size);
        }


        public static SourceCode LoadConfig(List<string> xpaths, string xmlfilepath)
        {
            return SourceCode.Load(xpaths, xmlfilepath);
        }

        public static SourceCode LoadConfig(string xmlfilepath)
        {
            return LoadConfig(null, xmlfilepath);
        }

    }
}
