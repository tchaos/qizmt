<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `EndsWith` #
`public bool EndsWith(mstring s)`

Returns true if this mstring ends with the mstring s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring er = mstring.Prepare("er");

    if (name.EndsWith(er))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool EndsWith(string s)`

Returns true if this mstring ends with the string s.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.EndsWith("er"))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```