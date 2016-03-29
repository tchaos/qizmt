<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToInt16` #
`public Int32 NextItemToInt16(char delimiter)`

Get the next Int16 item, delimited by the char delimiter.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int16 i1 = sLine.NextItemToInt16('|');
    mstring value = sLine.NextItemToString('|');
    Int16 i2 = sLine.NextItemToInt16('|');

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `NextItemToIntShort` #
`public Int32 NextItemToIntShort(char delimiter)`

Get the next Int16 item, delimited by the char delimiter.
### Remarks ###
This is equivalent to NextItemToInt16.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int16 i1 = sLine.NextItemToShort('|');
    mstring value = sLine.NextItemToString('|');
    Int16 i2 = sLine.NextItemToShort('|');

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```