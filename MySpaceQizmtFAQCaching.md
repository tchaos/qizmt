<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / <a href='Hidden comment: Link:'></a>[Qizmt FAQ](MySpaceQizmtFAQ.md)



# Qizmt FAQ - Caching #

## I have a mapreduce job, part of whose output is the count of values for a given key.  I would like to be able to sort my output based, in part, on this count.  Is there any way to do this, without running the data through another mapreduce job where the count is part of the key?  It seems to be overkill, as there would be no reduction, just new mapping. ##

---

This would have to be two mapreducers because any server could contribute to the count for any word so there is nothing to sort on until the count of each word comprehensively considers all words on all machines.

However, you can use mapreducer caching on both the word counter and the count sorter so that only delta gets exchanged/sorted each time you execute it.


<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_WordCountExample.png' alt='Qizmt Word Count Example' />


Qizmt-ExplicitCacheWordCount.xml is in the built-in examples and shows word count which only exchanges/sorts new texts in the cluster but producing a comprehensive word count after each iteration.