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
using System.Collections;
using System.Linq;
using System.Text;
namespace MySpace.DataMining.DistributedObjects
{
    public partial class MdoDistro
    {
        static private ulong BytesToULong(IList<byte> x, int offset)
        {
            ulong result = 0;
            switch (x.Count - offset)
            {
                case 0:
                    break;
                default:
                    result |= x[offset + 7];
                    goto case 7;
                case 7:
                    result |= (ulong)x[offset + 6] << 8;
                    goto case 6;
                case 6:
                    result |= (ulong)x[offset + 5] << 16;
                    goto case 5;
                case 5:
                    result |= (ulong)x[offset + 4] << 24;
                    goto case 4;
                case 4:
                    result |= (ulong)x[offset + 3] << 32;
                    goto case 3;
                case 3:
                    result |= (ulong)x[offset + 2] << 40;
                    goto case 2;
                case 2:
                    result |= (ulong)x[offset + 1] << 48;
                    goto case 1;
                case 1:
                    result |= (ulong)x[offset + 0] << 56;
                    break;
            }
            return result;
        }

        static private void ULongToBytes(ulong x, IList<byte> resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 56);
            resultbuf[bufoffset + 1] = (byte)(x >> 48);
            resultbuf[bufoffset + 2] = (byte)(x >> 40);
            resultbuf[bufoffset + 3] = (byte)(x >> 32);
            resultbuf[bufoffset + 4] = (byte)(x >> 24);
            resultbuf[bufoffset + 5] = (byte)(x >> 16);
            resultbuf[bufoffset + 6] = (byte)(x >> 8);
            resultbuf[bufoffset + 7] = (byte)x;
        }


        static private uint BytesToUInt(IList<byte> x, int offset)
        {
            uint result = 0;
            result |= (uint)x[offset + 0] << 24;
            result |= (uint)x[offset + 1] << 16;
            result |= (uint)x[offset + 2] << 8;
            result |= x[offset + 3];
            return result;
        }
        static private void UIntToBytes(uint x, IList<byte> resultbuf, int bufoffset)
        {
            resultbuf[bufoffset + 0] = (byte)(x >> 24);
            resultbuf[bufoffset + 1] = (byte)(x >> 16);
            resultbuf[bufoffset + 2] = (byte)(x >> 8);
            resultbuf[bufoffset + 3] = (byte)x;
        }


        static public List<List<byte>> GenerateRandomSamples_DenseInLowValues(int samples_per_category, int leading0s)
        {
            Random rnd = new Random();
            List<List<byte>> samples = new List<List<byte>>(5000);


            for (int i = 0; i < samples_per_category; i++)
            {
                List<byte> sample = new List<byte>(32);
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                sample.Add((byte)rnd.Next());
                samples.Add(sample);
            }

            return samples;
        }

    }


    public class cmprGlyphNodeNoUnEat : Comparer<GlyphNode>
    {
        public override int Compare(GlyphNode left, GlyphNode right)
        {
            return left.Glyph - right.Glyph;
        }
    }

    public class cmprGlyphNode : Comparer<GlyphNode>
    {
        public override int Compare(GlyphNode left, GlyphNode right)
        {
            for (int i = 0; i < left.leaf_word_ref.Count; i++)
            {
                int diff = left.leaf_word_ref[i] - right.leaf_word_ref[i];
                if (0 != diff)
                {
                    return diff;
                }
            }
            return 0;
        }
    }

}