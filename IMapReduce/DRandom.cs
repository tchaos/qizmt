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
    public class DRandom
    {
        private const int SAMPLE_SIZE = 1009;
        private const int MAX_PERIODS = 1039;   // 1MB/1009
        private int BYTE_SAMPLE_SIZE = SAMPLE_SIZE * 4;
        private int[] samples = new int[SAMPLE_SIZE];
        private int[] fixedRangeSamples = null;
        private int fixedRangeMax = 0;
        private int fixedRangeMin = 0;
        private bool isFixedRange = false;
        private byte[] bytes = null;
        private LCG generator = null;
        private int curSamplePos = -1;
        private int curBytePos = -1;
        private int periodCnt = 0;
        private bool useLCG = false;

        public DRandom(bool useLCGenerator, bool fixedRange, int minValue, int maxValue)
        {
            useLCG = useLCGenerator;
            if (useLCG)
            {
                generator = new LCG();
            }
            isFixedRange = fixedRange;
            if (isFixedRange)
            {
                if (minValue > maxValue)
                {
                    throw new ArgumentException("Error:  Invalid arguments for DRandom(): minValue cannot be greater than maxValue.");
                }
                fixedRangeSamples = new int[SAMPLE_SIZE];
                fixedRangeMin = minValue;
                fixedRangeMax = maxValue;
            }
            GenerateSamples();
        }

        public DRandom() : this(false, false, 0, 0) { }

        private void GenerateSamples()
        {
            periodCnt = 0;
            curSamplePos = -1;
            int seed = unchecked(System.DateTime.Now.Millisecond + System.Diagnostics.Process.GetCurrentProcess().Id);

            if (useLCG)
            {
                generator.GenerateSamples(seed, SAMPLE_SIZE, samples);
            }
            else
            {
                Random rnd = new Random(seed);
                for (int i = 0; i < SAMPLE_SIZE; i++)
                {
                    samples[i] = rnd.Next();
                }
            }
        }

        private void LoadBytes()
        {
            for (int i = 0; i < SAMPLE_SIZE; i++)
            {
                int x = samples[i];
                int j = i * 4;
                bytes[j] = (byte)(x >> 24);
                bytes[j + 1] = (byte)(x >> 16);
                bytes[j + 2] = (byte)(x >> 8);
                bytes[j + 3] = (byte)x;
            }
        }

        public byte NextByte()
        {
            if (bytes == null)
            {
                bytes = new byte[BYTE_SAMPLE_SIZE];
                LoadBytes();
            }
            if (++curBytePos >= BYTE_SAMPLE_SIZE)
            {
                if (++periodCnt >= MAX_PERIODS)
                {
                    GenerateSamples();
                    LoadBytes();
                }
                curBytePos = 0;
            }
            return bytes[curBytePos];
        }
        /*
        public int NextSmallRange(int minValue, int maxValue)
        {
            if (minValue > maxValue)
            {
                throw new ArgumentException("Error:  Invalid arguments for DRandom.NextSmallRange: minValue cannot be greater than maxValue.");
            }
            int dist = maxValue - minValue;
            double factor = (int)NextByte() * 0.00392156862745098; // NextByte() / 255
            return (int)(factor * dist + minValue);
        }
         * */

        public int Next()
        {
            if (++curSamplePos >= SAMPLE_SIZE)
            {
                if (++periodCnt >= MAX_PERIODS)
                {
                    GenerateSamples();
                }
                curSamplePos = 0;
            }
            if (isFixedRange)
            {
                if (periodCnt > 0)
                {
                    return fixedRangeSamples[curSamplePos];
                }
                long dist = (long)fixedRangeMax - (long)fixedRangeMin;
                double factor = (double)samples[curSamplePos] * 4.6566128752457969E-10; // num / Int32.MaxValue
                int num = (int)((long)(factor * dist + fixedRangeMin));
                fixedRangeSamples[curSamplePos] = num;
                return num;
            }
            else
            {
                return samples[curSamplePos];
            }
        }

        public int Next(int minValue, int maxValue)
        {
            if (minValue > maxValue)
            {
                throw new ArgumentException("Error:  Invalid arguments for DRandom.GetNextInt: minValue cannot be greater than maxValue.");
            }
            long dist = (long)maxValue - (long)minValue;
            double factor = (double)Next() * 4.6566128752457969E-10; // Next() / Int32.MaxValue
            return (int)((long)(factor * dist + minValue));
        }
        /*
        public int Next2(int minValue, int maxValue)
        {
            if (minValue > maxValue)
            {
                throw new ArgumentException("Error:  Invalid arguments for DRandom.GetNextInt: minValue cannot be greater than maxValue.");
            }
            long dist = (long)maxValue - (long)minValue;
            int num = Next();
            if (num % 2 == 0)
            {
                num = -num;
            }
            double factor = num;
            factor = (factor + 2147483646.0) / 4294967293d;
            return (int)((long)(factor * dist + minValue));
        }*/

        private class LCG
        {
            private int m = 0;
            private long a = 0;
            private long c = 0;

            public LCG()
            {
                m = 2147483647;
                a = 16807;
                c = 0;
            }

            public LCG(int modulus, long multiplier, long increment)
            {
                m = modulus;
                a = multiplier;
                c = increment;
            }

            public void GenerateSamples(int seed, int sampleSize, int[] samples)
            {
                int x = seed;
                for (int i = 0; i < sampleSize; i++)
                {
                    x = Proc(x);
                    samples[i] = x;
                }
            }

            private int Proc(int x)
            {
                return (int)((a * x + c) % m);
            }
        }
    }
}
