<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)


# `MPadLeft` #
`public mstring MPadLeft(int totalLength, char paddingChar)`

Pads this mstring on the left with the padding char for a specified total length.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.MPadLeft(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```

---




# `PadLeftM` #
`public mstring PadLeftM(int totalLength, char paddingChar)`

Pads this mstring on the left with the padding char for a specified total length.  This mstring is changed and returned.

### Remarks ###
This is equivalent to MPadLeft.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.PadLeftM(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```