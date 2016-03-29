<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `StartsWith` #
`public bool StartsWith(mstring s)`

Returns true if this mstring starts with the mstring s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M");

    if (name.StartsWith(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool StartsWith(string s)`

Returns true if this mstring starts with the string s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.StartsWith("M"))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```