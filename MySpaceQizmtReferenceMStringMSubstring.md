<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `MSubstring` #
`public mstring MSubstring(int startIndex, int length)`

Returns an instance of mstring starting at startIndex, with length number of characters.
### Remarks ###
remarks

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.MSubstring(0, 2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




`public mstring MSubstring(int startIndex)`

Returns an instance of mstring starting at startIndex, until the end of the mstring.
### Remarks ###
remarks

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.MSubstring(2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




# `SubstringM` #
`public mstring SubstringM(int startIndex, int length)`

Returns an instance of mstring starting at startIndex, with length number of characters.
### Remarks ###
This is equivalent to `MSubstring(int startIndex, int length)`.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    mstring sKey = ms.SubstringM(0, 2);
    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




`public mstring SubstringM(int startIndex)`

Returns an instance of mstring starting at startIndex, until the end of the mstring.
### Remarks ###
This is equivalent to `MSubstring(int startIndex)`.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.SubstringM(2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```