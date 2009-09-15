using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
namespace MySpace.DataMining.DistributedObjects
{
    public class MdoDistro
    {
        public uint i256 = 256;
        public int[] hash = new int[1000000 + 4294967295 / 256];
        public int m_iMajorCount = 1;
        public int m_iMinorCount = 1;
        public byte m_ignorebyte = (byte)'\0';
        public int m_ignoreposition = 0;
        public List<RangeItem> converted_samples;
        public List<RangeItem> converted_samples_hirarchical;
        public List<RangeItem> raw_samples;
        public uint modmax;

        public MdoDistro(List<List<byte>> samples, int iMajorCount, int iMinorCount)
        {
            modmax = (uint)(uint.MaxValue / (uint)i256) * (uint)i256;
            m_iMajorCount = iMajorCount;
            m_iMinorCount = iMinorCount;
            raw_samples = ConvertSamples(samples, out m_ignoreposition, out m_ignorebyte);
            raw_samples.Sort(new cmpr());
            Round256(raw_samples);
            converted_samples = EliminateDuplicates(raw_samples);
            int samples_min = m_iMajorCount * m_iMinorCount;
            if (converted_samples.Count < samples_min)
            {
                converted_samples_hirarchical = EliminateDuplicatesSubHash(raw_samples);
                FillGaps(converted_samples_hirarchical);
                converted_samples = FlattenList(converted_samples_hirarchical);
                PartitionMajorAndMinor(converted_samples, m_iMajorCount, m_iMinorCount);
                FillHash(converted_samples_hirarchical, hash);
                int i23zzzz = 23 + 23;
            }
            else
            {
                PartitionMajorAndMinor(converted_samples, m_iMajorCount, m_iMinorCount);
                FillHash(converted_samples, hash);
                int i23zzzz = 23 + 23;
            }
        }

        private void FillHashEmpties(int[] the_hash)
        {
            int lastloc = 0;
            for (int i = 0; i < the_hash.Length; i++)
            {
                if (the_hash[i] != -1)
                {
                    lastloc = the_hash[i];
                    break;
                }
            }
            for (int i = 0; i < the_hash.Length; i++)
            {
                if (the_hash[i] == -1)
                {
                    the_hash[i] = lastloc;
                }
                lastloc = the_hash[i];
            }
        }
        private void FillGaps(List<RangeItem> rConverted_Samples)
        {
            foreach (RangeItem sbg_hsh in rConverted_Samples)
            {
                int i23zzdzz = 23 + 23;
                FillHashEmpties(sbg_hsh.sub_hash);
                int i23zzzz = 23 + 23;
            }
            int i23zz = 23 + 23;
        }
        private void FillHash(List<RangeItem> rConverted_Samples, int[] the_hash)
        {
            for (int i = 0; i < the_hash.Length; i++)
            {
                the_hash[i] = -1;
            }
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                int iloc = (int)(rConverted_Samples[i].converted_sample256 / i256);
                //int iloc = (int)(rConverted_Samples[i].converted_sample256 % modmax);
                the_hash[iloc] = i;
            }
            FillHashEmpties(the_hash);
        }
        public bool m_bhirarchical = false;
        private List<RangeItem> EliminateDuplicatesSubHash(List<RangeItem> rConverted_Samples)
        {
            m_bhirarchical = true;
            List<RangeItem> rConverted_Samples_new = new List<RangeItem>(rConverted_Samples.Count);
            uint last256id = 0;
            int cur_rng = 0;
            int last_added_parent_idx = -1;
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                RangeItem pRangedItem = rConverted_Samples[i];
                if (pRangedItem.converted_sample256 != last256id)
                {
                    ++cur_rng;
                    pRangedItem.traverse_sub = true;
                    pRangedItem.sub_list = new List<RangeItem>((int)i256);
                    pRangedItem.sub_list.Add(pRangedItem);
                    pRangedItem.sub_hash = new int[(int)i256];
                    for (int j = 0; j < (int)i256; j++)
                    {
                        pRangedItem.sub_hash[j] = -1;
                    }
                    int loc_to_set = (int)pRangedItem.converted_sample % (int)i256;
                    pRangedItem.sub_hash[loc_to_set] = pRangedItem.sub_list.Count - 1;
                    ++last_added_parent_idx;
                    rConverted_Samples_new.Add(pRangedItem);
                }
                else
                {
                    RangeItem pParentRangeItem = rConverted_Samples_new[last_added_parent_idx];
                    pRangedItem.traverse_sub = false;
                    int loc_to_set = (int)pRangedItem.converted_sample % (int)i256;
                    if (pParentRangeItem.sub_hash[loc_to_set] == -1)
                    {
                        pParentRangeItem.sub_list.Add(pRangedItem);
                        pParentRangeItem.sub_hash[loc_to_set] = pParentRangeItem.sub_list.Count - 1;
                    }
                }
                last256id = pRangedItem.converted_sample256;
            }
            return rConverted_Samples_new;
        }


        private List<RangeItem> FlattenList(List<RangeItem> rConverted_Samples)
        {
            List<RangeItem> rConverted_Samples_new = new List<RangeItem>(rConverted_Samples.Count);

            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                RangeItem pRangedItem = rConverted_Samples[i];
                for (int j = 0; j < pRangedItem.sub_list.Count; j++)
                {
                    RangeItem pRangedItemSub = pRangedItem.sub_list[j];
                    pRangedItemSub.flat_loc = rConverted_Samples_new.Count;
                    rConverted_Samples_new.Add(pRangedItemSub);
                    pRangedItem.sub_list[j] = pRangedItemSub;
                }
            }
            return rConverted_Samples_new;
        }

        private List<RangeItem> EliminateDuplicates(List<RangeItem> rConverted_Samples)
        {
            List<RangeItem> rConverted_Samples_new = new List<RangeItem>(rConverted_Samples.Count);
            uint last256id = 0;
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                RangeItem pRangedItem = rConverted_Samples[i];
                if (pRangedItem.converted_sample256 != last256id)
                {
                    rConverted_Samples_new.Add(pRangedItem);
                }
                last256id = pRangedItem.converted_sample256;
            }
            return rConverted_Samples_new;
        }

        private List<RangeItem> CorrectDistribution(List<RangeItem> rConverted_Samples)
        {
            List<RangeItem> rConverted_Samples_new = new List<RangeItem>(rConverted_Samples.Count);
            uint last256id = 0;
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                RangeItem pRangedItem = rConverted_Samples[i];
                if (pRangedItem.converted_sample256 != last256id)
                {
                    rConverted_Samples_new.Add(pRangedItem);
                }
                last256id = pRangedItem.converted_sample256;
            }
            return rConverted_Samples_new;
        }



        private void Round256(List<RangeItem> rConverted_Samples)
        {
            for (int i = 0; i < rConverted_Samples.Count; i++)
            {
                RangeItem pRangedItem = rConverted_Samples[i];
                uint amount_to_add = i256 - pRangedItem.converted_sample % i256;
                if (((long)pRangedItem.converted_sample + (long)amount_to_add) > ((long)4294967295))
                {
                    pRangedItem.converted_sample256 = (4294967295 / i256) * i256;
                }
                else
                {
                    pRangedItem.converted_sample256 = pRangedItem.converted_sample + amount_to_add;
                }
                rConverted_Samples[i] = pRangedItem;
            }
        }

        private void PartitionMajorAndMinor(List<RangeItem> rConverted_Samples, int iMajorCount, int iMinorCount)
        {
            int chunkymax = 0;
            int iMajorPartitionSize = rConverted_Samples.Count / iMajorCount;
            int iMinorPartitionSize = rConverted_Samples.Count / (iMajorCount * iMinorCount);
            if (iMinorPartitionSize <= 0)
            {
                iMinorPartitionSize = 1;
            }
            if (iMajorPartitionSize <= 0)
            {
                iMajorPartitionSize = 1;
            }
            short cur_Major_id = 0;
            for (int i = 0; i < rConverted_Samples.Count; )
            {
                int chunkcurr = 0;
                short cur_Minor_id = 0;
                int j;
                for (j = i; j < i + iMajorPartitionSize && j < rConverted_Samples.Count; )
                {
                    int k;
                    for (k = j; k < j + iMinorPartitionSize && k < rConverted_Samples.Count && k < i + iMajorPartitionSize; k++)
                    {
                        RangeItem pRangeItem = rConverted_Samples[k];
                        pRangeItem.iMajorId = cur_Major_id;
                        pRangeItem.iMinorId = cur_Minor_id;
                        rConverted_Samples[k] = pRangeItem;
                    }
                    j = k;
                    if (cur_Minor_id < m_iMinorCount - 1)
                    {
                        ++cur_Minor_id;
                    }
                    else
                    {
                        ++chunkcurr;
                    }
                }
                if (chunkcurr > chunkymax)
                {
                    chunkymax = chunkcurr;
                }
                i = j;
                if (cur_Major_id < m_iMajorCount - 1)
                {
                    ++cur_Major_id;
                }
                else
                {
                    int i23zz = 23 + 23;
                }
            }
        }


        private List<RangeItem> ConvertSamples(List<List<byte>> samples, out int ignoreposition, out byte ignorebyte)
        {
            ignorebyte = 0;
            if (0 != samples.Count)
            {
                ignorebyte = samples[0][0];
            }
            List<RangeItem> converted_samples = new List<RangeItem>();
            int current_score = 0;
            int current_position = 0;
            /*
            Dictionary<uint, bool> tempDict = new Dictionary<uint, bool>(samples.Count);
            for (current_position = 0; current_position < samples[0].Count - 4; current_position++)
            {
                tempDict.Clear();
                for (int j = 0; j < samples.Count; j++)
                {
                    List<byte> sample = samples[j];
                    uint isample = BytesToUInt(sample, current_position);
                    tempDict[isample] = true;
                }
                current_score = tempDict.Count;
                if ((double)current_score > ((double)samples.Count * 0.80))
                { 
                    break;
                }
            }
            */
            ignoreposition = current_position;
            for (int j = 0; j < samples.Count; j++)
            {
                List<byte> sample = samples[j];
                bool bOutOfRange = false;
                for (int i = 0; i < ignoreposition; i++)
                {
                    if (sample[i] != ignorebyte)
                    {
                        bOutOfRange = true;
                        break;
                    }
                }
                if (bOutOfRange)
                {
                    break;
                }
                uint usample = BytesToUInt(sample, ignoreposition);
                RangeItem pRangeItem;
                pRangeItem.converted_sample = usample;
                pRangeItem.raw_sample = sample;
                pRangeItem.iMajorId = -1;
                pRangeItem.iMinorId = -1;
                pRangeItem.converted_sample256 = 0;
                pRangeItem.traverse_sub = false;
                pRangeItem.sub_hash = null;
                pRangeItem.sub_list = null;
                pRangeItem.flat_loc = -1;
                converted_samples.Add(pRangeItem);
            }
            return converted_samples;
        }

        public RangeItem Distro(IList<byte> key, int keyoffset, int keylength)
        {
            uint uikey = BytesToUInt(key, keyoffset + m_ignoreposition);
            int hash_location = (int)(uikey / i256);
            int index_location = hash[hash_location];
            RangeItem pRangeItem;
            if (m_bhirarchical)
            {
                pRangeItem = converted_samples_hirarchical[index_location];
                //int subhash_location = ((int)uikey - (int)pRangeItem.converted_sample256) - 1;
                int subhash_location = (int)uikey % (int)i256;
                int ilist_loc = pRangeItem.sub_hash[subhash_location];
                if (ilist_loc == -1)
                {
                    int i23zz = 23 + 23;
                }
                int iFlatLoc = pRangeItem.sub_list[ilist_loc].flat_loc;
                if (iFlatLoc == -1)
                {
                    int i23zz = 23 + 23;
                }
                pRangeItem = converted_samples[iFlatLoc];
            }
            else
            {
                pRangeItem = converted_samples[index_location];
            }
            return pRangeItem;
        }


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
                List<byte> sample = new List<byte>(8);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                sample.Add((byte)0);
                uint randval = (uint)rnd.Next(0, 420000);
                UIntToBytes(randval, sample, leading0s);
                samples.Add(sample);
            }
            /*
             for (int i = 0; i < samples_per_category; i++)
             {
                 List<byte> sample = new List<byte>(8);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 sample.Add((byte)0);
                 uint randval = (uint)rnd.Next();
                 UIntToBytes(randval, sample, leading0s);
                 samples.Add(sample);
             }
             */
            return samples;
        }

    }

    public class cmpr : Comparer<RangeItem>
    {
        public override int Compare(RangeItem left, RangeItem right)
        {
            if (left.converted_sample > right.converted_sample)
            {
                return 1;
            }
            else if (left.converted_sample < right.converted_sample)
            {
                return -1;
            }
            return 0;
        }
    }
    public struct RangeItem
    {
        public List<byte> raw_sample;
        public uint converted_sample;
        public uint converted_sample256;
        public short iMajorId;
        public short iMinorId;
        public bool traverse_sub;
        public int[] sub_hash;
        public List<RangeItem> sub_list;
        public int flat_loc;
    }
}
