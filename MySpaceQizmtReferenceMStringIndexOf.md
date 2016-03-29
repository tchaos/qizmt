<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `IndexOf` #
`public int IndexOf(string s)`

Returns the index of string s in this mstring.  If the string s is not found in this mstring, -1 is returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.IndexOf(" de ") > -1)
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public int IndexOf(mstring s)`

Returns the index of mstring s in this mstring.  If the mstring s is not found in this mstring, -1 is returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring de = mstring.Prepare(" de ");

    if (name.IndexOf(de) > -1)
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public int IndexOf(char c)`

Returns the index of char c in this mstring.  If the char is not found, -1 is returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM('.').TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16 && word.IndexOf('.') == 0)
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```