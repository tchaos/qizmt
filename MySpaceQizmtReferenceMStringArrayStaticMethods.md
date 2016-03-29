<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)


# Static Methods of mstringarray #



## `Prepare` ##
`public static mstringarray Prepare()`

Returns an empty mstringarray with Length = 0.

#### Example ####
```
mstringarray arr2 = mstringarray.Prepare();

if(id == 54)
{
    arr2 = mstringarray.Prepare(10);
}

if(id == 778)
{
    arr2 = mstringarray.Prepare(19);
} 
```

---




`public static mstringarray Prepare(int length)`

Returns a mstringarray with the specified length.

#### Example ####
```
mstringarray arr2 = mstringarray.Prepare();

if(id == 54)
{
    arr2 = mstringarray.Prepare(10);
}

if(id == 778)
{
    arr2 = mstringarray.Prepare(19);
} 
```