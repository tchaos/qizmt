<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToUInt16` #
`public UInt16 ToUInt16()`

Converts this mstring to UInt16.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt16 i1 = parts[0].ToUInt16();
    mstring value = parts[1];
    UInt16 i2 = parts[2].ToUInt16();

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `ToUShort` #
`public UInt16 ToUShort()`

Converts this mstring to UInt16.
### Remarks ###
This is equivalent to ToUInt16.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt16 i1 = parts[0].ToUShort();
    mstring value = parts[1];
    UInt16 i2 = parts[2].ToUShort();

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```