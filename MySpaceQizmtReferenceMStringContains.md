<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `Contains` #
`public bool Contains(char c)`

Returns true if this mstring contains the char c.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.Contains('M'))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool Contains(mstring s)`

Returns true if this mstring contains the mstring s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M.");

    if (name.Contains(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool Contains(string s)`

Returns true if this mstring contains the string s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M.");

    if (name.Contains(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```