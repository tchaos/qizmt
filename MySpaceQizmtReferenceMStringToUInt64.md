<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToUInt64` #
`public UInt64 ToUInt64()`

Converts this mstring to UInt64.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt64 i1 = parts[0].ToUInt64();
    mstring value = parts[1];
    UInt64 i2 = parts[2].ToUInt64();

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```

---




# `ToULong` #
`public UInt64 ToULong()`

Converts this mstring to UInt64.
### Remarks ###
This is equivalent to ToUInt64.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt64 i1 = parts[0].ToULong();
    mstring value = parts[1];
    UInt64 i2 = parts[2].ToULong();

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```