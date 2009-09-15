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
            string s = text.TrimStart(' ', '\t', '\r', '\n');
            if (s.Length > 0)
            {
                if (s[0] == '(' || s[0] == ')' || s[0] == ',' || s[0] == ';')
                {
                    string result = s.Substring(0, 1);
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
                                string result = s;
                                text = "";
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
                                string result = s.Substring(0, i);
                                text = s.Substring(i + 1);
                                return result;
                            }
                            else // Text directly after.
                            {
                                string result = s.Substring(0, i);
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
                    string result = s;
                    text = string.Empty;
                    return result;
                }
                if (char.IsWhiteSpace(s[i]))
                {
                    string result = s.Substring(0, i);
                    text = s.Substring(i + 1);
                    return result;
                }
                if (!char.IsLetterOrDigit(s[i]) && '_' != s[i] && '.' != s[i])
                {
                    if (i > 0)
                    {
                        string result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                    {
                        i++; // Return this symbol.
                        string result = s.Substring(0, i);
                        text = s.Substring(i);
                        return result;
                    }
                }


            }
        }
    }
}
