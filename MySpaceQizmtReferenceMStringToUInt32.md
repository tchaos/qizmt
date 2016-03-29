<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToUInt32` #
`public UInt32 ToUInt32()`

Converts this mstring to UInt32.

### Example ###
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt32 i1 = parts[0].ToUInt32();
    mstring value = parts[1];
    UInt32 i2 = parts[2].ToUInt32();

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `ToUInt` #
`public UInt32 ToUInt()`

Converts this mstring to UInt32.
### Remarks ###
This is equivalent to ToUInt32.

### Example ###
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt32 i1 = parts[0].ToUInt();
    mstring value = parts[1];
    UInt32 i2 = parts[2].ToUInt();

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```