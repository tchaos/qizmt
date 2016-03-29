<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `ResetGetPosition` #
`public void ResetGetPosition()`

Reset the get position to the very first beginning.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 i1 = sLine.NextItemToInt64('|');
    Int64 i2 = sLine.NextItemToInt64('|');
    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);
    mstring value = GetValue(sline);

    output.Add(key, value);
}

private mstring GetValue(mstring sline)
{
    sline.ResetGetPosition();
    Int64 num = sline.NextItemToInt64('|');
    num = sline.NextItemToInt64('|');
    mstring name = sline.NextItemToString('|');
    mstring id = sline.NextItemToString('|');
    name = name.AppendM(id);
    return name;
} 
```