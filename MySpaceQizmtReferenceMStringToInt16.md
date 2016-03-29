<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToInt16` #
`public Int16 ToInt16()`

Converts this mstring to Int16.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int16 i1 = parts[0].ToInt16();
    mstring value = parts[1];
    Int16 i2 = parts[2].ToInt16();

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```

---




# `ToShort` #
`public Int16 ToShort()`

Converts this mstring to Int16.
### Remarks ###
This is equivalent to ToInt16.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int16 i1 = parts[0].ToShort();
    mstring value = parts[1];
    Int16 i2 = parts[2].ToShort();

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```