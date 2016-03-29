<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `MReplace` #
`public mstring MReplace(char oldChar, char newChar)`

Replace all occurances of old char with the new char.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.MReplace('.', '-');
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(ref mstring oldString, ref mstring newString)`

Replace all occurances of old mstring with the new mstring.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newS = mstring.Prepare("");
    mstring newName = name.MReplace(ref oldS, ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(ref mstring oldString, string newString)`

Replace all occurances of old mstring with the new string.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newName = name.MReplace(ref oldS, "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(string oldString, ref mstring newString)`

Replace all occurances of old string with the new mstring.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newS = mstring.Prepare("");
    mstring newName = name.MReplace("Mr.", ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(string oldString, string newString)`

Replace all occurances of old string with the new string.  This mstring is changed and returned.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.MReplace("Mr.", "");
    output.Add(newName, mstring.Prepare());
} 
```

---




# `ReplaceM` #
`public mstring ReplaceM(char oldChar, char newChar)`

Replace all occurances of old char with the new char.  This mstring is changed and returned.
### Remarks ###
This is equivalent to MReplace.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.ReplaceM('.', '-');
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(ref mstring oldString, string newString)`

Replace all occurances of old mstring with the new string.  This mstring is changed and returned.
### Remarks ###
This is equivalent to MReplace.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newName = name.ReplaceM(ref oldS, "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(string oldString, ref mstring newString)`

Replace all occurances of old string with the new mstring.  This mstring is changed and returned.
### Remarks ###
This is equivalent to MReplace.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newS = mstring.Prepare("");
    mstring newName = name.ReplaceM("Mr.", ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(string oldString, string newString)`

Replace all occurances of old string with the new string.  This mstring is changed and returned.
### Remarks ###
This is equivalent to MReplace.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.ReplaceM("Mr.", "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(ref mstring oldString, ref mstring newString)`

Replace all occurances of old mstring with the new mstring.  This mstring is changed and returned.
### Remarks ###
This is equivalent to MReplace.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newS = mstring.Prepare("");
    mstring newName = name.ReplaceM(ref oldS, ref newS);
    output.Add(newName, mstring.Prepare());
} 
```