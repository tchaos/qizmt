<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToUInt16` #
`public Int32 NextItemToUInt16(char delimiter)`

Get the next UInt16 item, delimited by the char delimiter.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt16 i1 = sLine.NextItemToUInt16('|');
    mstring value = sLine.NextItemToString('|');
    UInt16 i2 = sLine.NextItemToUInt16('|');

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `NextItemToUShort` #
`public Int32 NextItemToUShort(char delimiter)`

Get the next UInt16 item, delimited by the char delimiter.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt16 i1 = sLine.NextItemToUShort('|');
    mstring value = sLine.NextItemToString('|');
    UInt16 i2 = sLine.NextItemToUShort('|');

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```