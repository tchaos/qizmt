using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySpace.DataMining.DistributedObjects
{

    class Program
    {

        class Word: IComparable<Word>
        {
            internal string word;
            internal GlyphNode range;

            public int  CompareTo(Word that)
            {
                return string.Compare(this.word, that.word, StringComparison.Ordinal);
            }

        }

        static void RunWordTest()
        {

            const int KeyLength = 16;
            const uint iSlaveCount = 1;
            const uint iZblocksPerSlave = 113;

            List<List<byte>> samples = new List<List<byte>>(1000);
            foreach (string part in MySpaceText.Split('\n')[0].Split(' ')) // Sample from first line.
            {
                string word = part.Trim(WordTrimChars).ToLower();
                if (0 != word.Length)
                {
                    List<byte> key = new List<byte>(KeyLength);
                    key.AddRange(Encoding.ASCII.GetBytes(word.PadRight(KeyLength, '\0')));
                    samples.Add(key);
                }
            }
            MdoDistro pCRangeEstimator = new MdoDistro(samples, iSlaveCount, iZblocksPerSlave);

            {
                Dictionary<string, List<GlyphNode>> results = new Dictionary<string, List<GlyphNode>>();
                List<Word> words = new List<Word>(1000);
                foreach (string part in MySpaceText.Split(' ', '\n'))
                {
                    string word = part.Trim(WordTrimChars).ToLower();
                    if (0 != word.Length)
                    {
                        List<byte> key = new List<byte>(KeyLength);
                        key.AddRange(Encoding.ASCII.GetBytes(word.PadRight(KeyLength, '\0')));

                        GlyphNode pRangeItem = pCRangeEstimator.Distro(key);
                        if (pRangeItem.iMajorId >= iSlaveCount)
                        {
                            throw new Exception("pRangeItem.iMajorId >= iSlaveCount");
                        }
                        if (pRangeItem.iMinorId >= iZblocksPerSlave)
                        {
                            throw new Exception("pRangeItem.iMinorId >= iZblocksPerSlave");
                        }

                        Word w = new Word();
                        w.word = word;
                        w.range = pRangeItem;
                        words.Add(w);

                        string sExchangePath = @"" + pRangeItem.iMajorId.ToString() + @"_" + pRangeItem.iMinorId + "";
                        if (!results.ContainsKey(sExchangePath))
                        {
                            results[sExchangePath] = new List<GlyphNode>();
                        }
                        results[sExchangePath].Add(pRangeItem);
                    }
                }
                words.Sort();

                int i333 = 33 + 33;

                {
                    Console.WriteLine("Testing WordTest...");
                    int lastmajor = 0;
                    int lastminor = 0;
                    for (int i = 0; i < words.Count; i++)
                    {
                        if (words[i].word == "create")
                        {
                            int i3323 = 33 + 33;
                        }
                        if (words[i].range.iMajorId < lastmajor)
                        {
                            throw new Exception("Whoops!");
                        }
                        if (words[i].range.iMajorId != lastmajor)
                        {
                            lastminor = 0;
                        }
                        if (words[i].range.iMinorId < lastminor)
                        {
                            while (true)
                            {
                                GlyphNode ppRangeItem1 = pCRangeEstimator.Distro("create".PadRight(16, '\0'));
                                GlyphNode ppRangeItem2 = pCRangeEstimator.Distro("day".PadRight(16, '\0'));
                                GlyphNode ppRangeItem3 = pCRangeEstimator.Distro("decide".PadRight(16, '\0'));
                                int i23zzz = 23 + 23;
                            }
                            throw new Exception("Whoops!");
                        }
                        lastmajor = words[i].range.iMajorId;
                        lastminor = words[i].range.iMinorId;
                    }
                    Console.WriteLine("Done with WordTest!");
                }

                {
                    Console.WriteLine("Testing WordTest...");
                    byte last = 0;
                    byte lastn = 0;
                    for (int major = 0; major < iSlaveCount; major++)
                    {
                        for (int minor = 0; minor < iZblocksPerSlave; minor++)
                        {
                            string key = major.ToString() + "_" + minor.ToString();
                            if (results.ContainsKey(key))
                            {
                                results[key].Sort(new cmprGlyphNode());
                                for (int ig = 0; ig < results[key].Count; ig++)
                                {
                                    byte current = results[key][ig].leaf_word_ref[0];
                                    byte currentn = results[key][ig].leaf_word_ref[1];
                                    if (current < last)
                                    {
                                        throw new Exception("Whoops");
                                    }
                                    if (current != last)
                                    {
                                        lastn = 0;
                                    }
                                    if (currentn < lastn)
                                    {
                                        throw new Exception("Whoops");
                                    }
                                    last = current;
                                    lastn = currentn;
                                }
                            }
                        }
                    }
                    Console.WriteLine("Done with WordTest!");
                }

                int i2233zz = 23 + 23;
            }

        }


        static void Main(string[] args)
        {
            RunWordTest();

            List<List<byte>> samples = MdoDistro.GenerateRandomSamples_DenseInLowValues(100000, 0);
            uint iSlaveCount = 31;
            uint iZblocksPerSlave = 113;
            MdoDistro pCRangeEstimator = new MdoDistro(samples, iSlaveCount, iZblocksPerSlave);
            Random rnd = new Random();
            long total_major = 0;
            long total_minor = 0;
            List<List<int>> lii = new List<List<int>>((int)iSlaveCount);
            for (int i = 0; i < iSlaveCount; i++)
            {
                List<int> ilist = new List<int>((int)iZblocksPerSlave);
                lii.Add(ilist);
                for (int j = 0; j < iZblocksPerSlave; j++)
                {
                    ilist.Add(0);
                }
            }
            Dictionary<string, List<GlyphNode>> results = new Dictionary<string, List<GlyphNode>>();
            for (int i = 0; i < 100000; i++)
            {
                GlyphNode pRangeItem = pCRangeEstimator.Distro(samples[rnd.Next(0, samples.Count)]);
                if (pRangeItem.iMajorId >= iSlaveCount)
                {
                    throw new Exception("pRangeItem.iMajorId >= iSlaveCount");
                }
                if (pRangeItem.iMinorId >= iZblocksPerSlave)
                {
                    throw new Exception("pRangeItem.iMinorId >= iZblocksPerSlave");
                }
                string sExchangePath = @"" + pRangeItem.iMajorId.ToString() + @"_" + pRangeItem.iMinorId + "";
                total_major += pRangeItem.iMajorId;
                total_minor += pRangeItem.iMinorId;
                lii[pRangeItem.iMajorId][pRangeItem.iMinorId]++;
                if (!results.ContainsKey(sExchangePath))
                {
                    results[sExchangePath] = new List<GlyphNode>();
                }
                results[sExchangePath].Add(pRangeItem);

            }

            double majorpercent = ((double)(total_major / 100000)) / iSlaveCount;
            double minorpercent = ((double)(total_minor / 100000)) / iZblocksPerSlave;

            {
                Console.WriteLine("Testing...");
                byte last = 0;
                byte lastn = 0;
                for (int major = 0; major < iSlaveCount; major++)
                {
                    for (int minor = 0; minor < iZblocksPerSlave; minor++)
                    {
                        string key = major.ToString() + "_" + minor.ToString();
                        if (results.ContainsKey(key))
                        {
                            results[key].Sort(new cmprGlyphNode());
                            for (int ig = 0; ig < results[key].Count; ig++)
                            {
                                byte current = results[key][ig].leaf_word_ref[0];
                                byte currentn = results[key][ig].leaf_word_ref[1];
                                if (current < last)
                                {
                                    throw new Exception("Whoops");
                                }
                                if (current != last)
                                {
                                    lastn = 0;
                                }
                                if (currentn < lastn)
                                {
                                    throw new Exception("Whoops");
                                }
                                last = current;
                                lastn = currentn;
                            }
                        }
                    }
                }
                Console.WriteLine("Done with test!");
            }

            int i2233zz = 23 + 23;
        }




        static char[] WordTrimChars = new char[] { '\r', '\t', '.', '!', '?', ',', '\'', '"', ')', '(' };

        const string MySpaceText = @"Create a community on MySpace and you can share photos, journals and interests with your growing network of mutual friends!

See who knows who, or how you are connected. Find out if you really are six people away from Kevin Bacon.

MySpace is for everyone:
Friends who want to talk Online 
Single people who want to meet other Singles 
Matchmakers who want to connect their friends with other friends 
Families who want to keep in touch--map your Family Tree 
Business people and co-workers interested in networking 
Classmates and study partners 
Anyone looking for long lost friends!


MySpace makes it easy to express yourself, connect with friends and make new ones, but please remember that what you post publicly can be read by anyone viewing your profile, so we suggest you consider the following guidelines when using MySpace: 
Don't forget that your profile and MySpace forums are public spaces. Don't post anything you wouldn't want the world to know (e.g., your phone number, address, IM screens name, or specific whereabouts). Avoid posting anything that would make it easy for a stranger to find you, such as where you hang out every day after school. 
People aren't always who they say they are. Be careful about adding people you don't know in the physical world to your friends list. It's fun to connect with new MySpace friends from all over the world, but avoid meeting people in person whom you do not already know in the physical world. If you decide to meet someone you've met on MySpace, tell your parents first, do it in a public place and bring a trusted adult. 
Harassment, hate speech and inappropriate content should be reported. If you feel someone's behavior is inappropriate, react. Talk with a trusted adult, or report it to MySpace or the authorities.
Don't post anything that would embarrass you later. It's easy to think that only people you know are looking at your MySpace page, but the truth is that everyone can see it. Think twice before posting a photo or information you wouldn't want others to see, including potential employers or colleges!
Do not lie about your age.  Your profile may be deleted and your Membership may be terminated without warning if we believe that you are under 14 years of age or if we believe you are 14 through 17 years of age and you represent yourself as 18 or older.       
Don't get hooked by a phishing scam.  Phishing is a method used by fraudsters to try to get your personal information, such as your username and password, by pretending to be a site you trust. Click here to learn more.";


    }
}
