<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ToInt32` #
`public Int32 ToInt32()`

Converts this mstring to Int32.

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

    int i1 = parts[0].ToInt32();
    mstring value = parts[1];
    int i2 = parts[2].ToInt32();

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




# `ToInt` #
`public Int32 ToInt()`

Converts this mstring to Int32.
### Remarks ###
This is equivalent to ToInt32().

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

    int i1 = parts[0].ToInt();
    mstring value = parts[1];
    int i2 = parts[2].ToInt();

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```