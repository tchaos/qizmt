using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryAnalyzer_DataProvider
{
    class Qa
    {
        public static string NextPart(ref string text)
        {
            StringSlice ssText = StringSlice.Prepare(text);
            StringSlice ssResult = SliceNextPart(ref ssText);
            text = ssText.ToString();
            return ssResult.ToString();
        }


        public struct StringSlice
        {
            private string str;
            private int offset;
            private int length;


            public string EntireString
            {
                get
                {
                    return this.str;
                }
            }

            public int Offset
            {
                get
                {
                    return this.offset;
                }
            }

            public int Length
            {
                get
                {
                    return this.length;
                }
            }

            public void GetComponents(out string str, out int offset, out int length)
            {
                str = this.str;
                offset = this.offset;
                length = this.length;
            }

            public static StringSlice Prepare(string str)
            {
                StringSlice ssnew;
                ssnew.str = str;
                ssnew.offset = 0;
                ssnew.length = str.Length;
                return ssnew;
            }

            public static StringSlice Prepare(string str, int offset, int length)
            {
                StringSlice ssnew;
                ssnew.str = str;
                ssnew.offset = offset;
                ssnew.length = length;
                return ssnew;
            }

            public static StringSlice Prepare(StringSlice ss, int offset, int length)
            {
                if (offset < 0 || length < 0 || offset + length > ss.length)
                {
                    IndexOutOfRangeException ior = new IndexOutOfRangeException("StringSlice.Prepare(StringSlice{offset=" + ss.offset + ", length=" + ss.length + "}, offset=" + offset + ", length=" + length + ")");
                    throw new ArgumentOutOfRangeException("Specified argument was out of the range of valid values. Index out of bounds: " + ior.Message, ior); // Preserve the old exception type.
                }
                StringSlice ssnew;
                ssnew.str = ss.str;
                ssnew.offset = ss.offset + offset;
                ssnew.length = length;
                return ssnew;
            }

            public static StringSlice Prepare()
            {
                StringSlice ssnew;
                ssnew.str = "";
                ssnew.offset = 0;
                ssnew.length = 0;
                return ssnew;
            }

            public StringSlice Substring(int startIndex)
            {
                return StringSlice.Prepare(this, startIndex, this.Length - startIndex);
            }

            public StringSlice Substring(int startIndex, int length)
            {
                return StringSlice.Prepare(this, startIndex, length);
            }

            public void AppendTo(StringBuilder sb)
            {
                sb.Append(this.str, this.offset, this.length);
            }

            public override string ToString()
            {
                if (0 == offset
                    && length == str.Length)
                {
                    return str;
                }
                return str.Substring(offset, length);
            }

            public char this[int index]
            {
                get
                {
                    if (index < 0 || index >= this.length)
                    {
                        throw new ArgumentOutOfRangeException();
                    }
                    return this.str[this.offset + index];
                }
            }

            public static int Compare(StringSlice strA, string strB)
            {
                return string.Compare(strA.str, strA.offset, strB, 0, Math.Max(strA.length, strB.Length));
            }

            public static int Compare(StringSlice strA, string strB, int strBindex, int length)
            {
                return string.Compare(strA.str, strA.offset, strB, strBindex, length);
            }

            public static int Compare(StringSlice strA, string strB, StringComparison comparisonType)
            {
                return string.Compare(strA.str, strA.offset, strB, 0, Math.Max(strA.length, strB.Length), comparisonType);
            }

            public static int Compare(StringSlice strA, string strB, int strBindex, int length, StringComparison comparisonType)
            {
                return string.Compare(strA.str, strA.offset, strB, strBindex, length, comparisonType);
            }

            public StringSlice TrimStart(params char[] trimchars)
            {
                int end = this.offset + this.length;
                int trimcharsLength = trimchars.Length;
                for (int i = offset; i != end; i++)
                {
                    for (int j = 0; ; j++)
                    {
                        if (j == trimcharsLength)
                        {
                            return StringSlice.Prepare(this.str, i, end - i);
                        }
                        if (this.str[i] == trimchars[j])
                        {
                            break;
                        }
                    }
                }
                return StringSlice.Prepare(this.str, this.length, this.length);
            }

            public static StringSlice Concat(StringSlice ssA, StringSlice ssB)
            {
                if (object.ReferenceEquals(ssA.str, ssB.str))
                {
                    if (ssA.offset + ssA.length == ssB.offset)
                    {
                        return StringSlice.Prepare(ssA.str, ssA.offset, ssA.length + ssB.length);
                    }
                }
                return StringSlice.Prepare(ssA.ToString() + ssB.ToString());
            }

            public static StringSlice Concat(StringSlice ssA, string sB)
            {
                return StringSlice.Prepare(ssA.ToString() + sB);
            }

        }

        private static char[] MyWhitespace = new char[] { ' ', '\t', '\r', '\n' };

        public static StringSlice SliceNextPart(ref StringSlice text)
        {
            StringSlice s = text.TrimStart(MyWhitespace);
            if (s.Length > 0)
            {
                if (s[0] == '(' || s[0] == ')' || s[0] == ',' || s[0] == ';')
                {
                    StringSlice result = s.Substring(0, 1);
                    text = s.Substring(1);
                    return result;
                }
                if (s[0] == '\'')
                {
                    bool prevsquot = false;
                    for (int i = 1; ; i++)
                    {
                        if (i >= s.Length)
                        {
                            if (prevsquot)
                            {
                                StringSlice result = s;
                                text = StringSlice.Prepare();
                                return result;
                            }
                            throw new Exception("Expected terminating single quote: " + s);
                        }
                        if (s[i] == '\'')
                        {
                            if (prevsquot)
                            {
                                prevsquot = false;
                            }
                            else
                            {
                                prevsquot = true;
                            }
                        }
                        else if (prevsquot)
                        {
                            if (s[i] == ' ')
                            {
                                StringSlice result = s.Substring(0, i);
                                text = s.Substring(i + 1);
                                return result;
                            }
                            else // Text directly after.
                            {
                                StringSlice result = s.Substring(0, i);
                                text = s.Substring(i);
                                return result;
                            }
                        }
                    }
                }
            }
            for (int i = 0; ; i++)
            {
                if (i >= s.Length)
                {
                    StringSlice result = s;
                    text = StringSlice.Prepare();
                    return result;
                }
                if (char.IsWhiteSpace(s[i]))
                {
                    StringSlice result = s.Substring(0, i);
                    text = s.Substring(i + 1);
                    return result;
                }
                if (!char.IsLetterOrDigit(s[i]) && '_' != s[i] && '.' != s[i])
                {
                    if (i > 0)
                    {
                        StringSlice result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                    {
                        i++; // Return this symbol.
                        StringSlice result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                }


            }
        }


        public static Int64 Int64Parse(Qa.StringSlice ss)
        {
            if (ss.Length < 1)
            {
                throw new FormatException("Invalid format for Int64",
                    new ArgumentException("Empty string provided", "ss"));
            }

            int offset = 0;
            int length = ss.Length;

            bool neg = false;
            if ('-' == ss[offset])
            {
                neg = true;
                offset++;
            }

            Int64 result = 0;
            unchecked
            {
                for (; offset < length; offset++)
                {
                    char by = ss[offset];
                    if (by >= '0' && by <= '9')
                    {
                        Int64 oldval = result;
                        result *= 10;
                        result -= (byte)by - '0';
                        if (result > oldval)
                        {
                            throw new OverflowException("Arithmetic operation resulted in an overflow: (Int64)" + ss);
                        }
                    }
                    else
                    {
                        //offset++;
                        //break;
                        throw new FormatException("Invalid character found: (Int64)" + ss);
                    }
                }
            }
            if (!neg)
            {
                result = Math.Abs(result);
            }
            return result;
        }

        public static Int32 Int32Parse(Qa.StringSlice ss)
        {
            if (ss.Length < 1)
            {
                throw new FormatException("Invalid format for Int32",
                    new ArgumentException("Empty string provided", "ss"));
            }

            int offset = 0;
            int length = ss.Length;

            bool neg = false;
            if ('-' == ss[offset])
            {
                neg = true;
                offset++;
            }

            Int32 result = 0;
            unchecked
            {
                for (; offset < length; offset++)
                {
                    char by = ss[offset];
                    if (by >= '0' && by <= '9')
                    {
                        Int32 oldval = result;
                        result *= 10;
                        result -= (byte)by - '0';
                        if (result > oldval)
                        {
                            throw new OverflowException("Arithmetic operation resulted in an overflow: (Int32)" + ss);
                        }
                    }
                    else
                    {
                        //offset++;
                        //break;
                        throw new FormatException("Invalid character found: (Int32)" + ss);
                    }
                }
            }
            if (!neg)
            {
                result = Math.Abs(result);
            }
            return result;
        }


    }
}
