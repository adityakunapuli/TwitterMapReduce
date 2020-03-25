---
layout: page
title: Information Retrieval
subtitle: Generating a $\text{tf-idf}$ index using MapReduce.
---
<img class="centerimg" src="/img/project1/logo_mapreduce.webp">


<p class="myquote">
The culmination of the Information Retrieval class was a group project in which we were required to construct a working search engine on a sample dataset of our choice (we used Twitter).  My portion of this project involved gathering the dataset from Twitter, and generating an TF-iDF scored term index using Hadoop MapReduce. <br>
</p>


The table below contains links to the respective sections as well as the corresponding piece of code I authored.
<table class="tg">
<tr style="border-bottom: 1px solid black; border-top: 0px solid white">
  <th class="col1 bld">Section</th>
  <th class="col2 bld">Code</th>
</tr>
  <tr style="background-color: white;">
    <th class="col1"><a href="#part1">Gathering the data</a></th>
    <th class="col2"><a href="https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/twcrawler.py">Twitter Crawler Code (GitHub)</a></th>
  </tr>
  <tr style="background-color: white;">
  <th class="col1"><a href="#part2">Creating the index</a></th>
  <th class="col2"><a href="https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/mrPhase_Final.java">TF-IDF MapReduce Code (GitHub)</a></th>
  </tr>
</table>


<!-- The final output of our combined efforts yielded a search engine built using `react.js`, and can be seen in the screenshots below.

* * *

<p class="caption">Search results of Tweets using the query "the kids basketball".</p>
![Search Engine Output](/assets/code/mr/Example1.png "Search Engine Output")

* * *

<p class="caption">Search results for query "superbowl sport" shown across a map (this only works for Tweets with associated geolocation enabled).</p>
![Map of Results](/assets/code/mr/Example2.png "Map of Results")

* * *

<p class="caption">Timeline of search results for query "hollywood california".</p>
![Timeline of Results](/assets/code/mr/Example3.png "Timeline of Results")

* * * -->

# <a name="part1"></a> Part 1: Gathering Data
<!-- <div style="text-align:center; width=768px;">
  <a href="/assets/code/mr/mrPhase_Final.java">
    <input  type="button"
            class="bigButton"
            value="PyTweet Crawler Code (GitHub)"
            href="/assets/code/mr/twcrawler.py"/>
  </a>
</div> -->

For the dataset we chose Twitter as it offers a large and diverse corpus.  Though in retrospect, this may have been a poor choice as I ended up spending an _inordinate_ amount of time just cleaning up the tweets themselves.  Every time I thought I had finally gotten the perfect set of RegEx to catch everything, a new edge case would pop up to ruin my day.

<!-- ```python
                    # Usernames
tweet_text = re.sub('(@[A-Za-z0-9_]+)|'
                    # Hashtags/topics
                    '(#[0-9A-Za-z]+)|'
                    # Emojis
                    '[^\x00-\x7F]|'
                    # URLs
                    'http[s]?\:\/\/.[a-zA-Z0-9\.\/\-]+'
                    , ' ', raw_tweet_text)
                    # Non-alpha-numeric
tweet_text = re.sub('([^0-9A-Za-z \t])', '', tweet_text)
tweet_text = tweet_text.strip()
``` -->

```python
tweet_text = re.sub('(@[A-Za-z0-9_]+)|'     # Usernames
                    '(#[0-9A-Za-z]+)|'      # Hashtags/topics
                    '[^\x00-\x7F]|'         # Emojis
                    'http[s]?\:\/\/.[a-zA-Z0-9\.\/\-]+', ' ', raw_tweet_text) # URLs
tweet_text = re.sub('([^0-9A-Za-z \t])', '', tweet_text)  # Non-alpha-numeric
tweet_text = tweet_text.strip()
```


This issue would crop up again later when it came time to actually index the tweets (though in a different way).  I'll go into that in the next section.

The diagram below shows the basic architecture of my crawler.  For the sake of efficiency, we had two crawlers running with mutually exclusive geographic bounding boxes--i.e. neither crawler would capture the other's tweets.  The tweets were fed into a SQLite3 database as I was still fairly paranoid about missing some CSV breaking characters with my aforementioned RegEx.

<!-- <img class="centerimg" src="/img/project1/Crawler-Architecture.jpg"> -->
<img class="centerimg" src="/img/project1/Crawler-Architecture.webp">


The end result was that we managed to grab over 10 million tweets for our dataset.  A sample subset is shown below.

<table class="tableizer-table">
   <thead>
      <tr class="tableizer-firstrow">
         <th>id_str</th>
         <th>text</th>
         <th>rawtext</th>
      </tr>
   </thead>
   <tbody>
      <tr>
         <td style="white-space: nowrap;">1094835700699222016</td>
         <td style="text-align:left;">Yeehaw Welcome home Elder Coulson Harris Salt Lake City International Airport SLC</td>
         <td style="text-align:left;">Yee-haw!!! Welcome home Elder Coulson Harris #Texas #sanantonio #returnwithhonor @ Salt Lake City International Airport (SLC) https://t.co/UW0adG331S</td>
      </tr>
      <tr>
         <td style="white-space: nowrap;">1094809801878470656</td>
         <td style="text-align:left;">Thanking my Kenyan friends for keeping me warm in Seattle Seattle Washington</td>
         <td style="text-align:left;">Thanking my Kenyan friends for keeping me warm in Seattle! @OngwenMartin @ Seattle Washington https://t.co/Z0opfjxdwT</td>
      </tr>
      <tr>
         <td style="white-space: nowrap;">1094809820375396352</td>
         <td style="text-align:left;">I ll be o your radio tonight 10Midnight Turn the dial to Donut Bar Las Vegas</td>
         <td style="text-align:left;">I'll be o your radio tonight 10-Midnight. Turn the dial to @hot975vegas #billiondollarbeard #zeshbian @ Donut Bar Las Vegas https://t.co/6qrBcdsmHC</td>
      </tr>
   </tbody>
</table>

A proper subset of 500 rows (including all columns) can [viewed here (GitHub)](https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/tweets_10K_subset.csv).

# <a name="part2"></a> Part 2: Index Creation

<p class="myquote">
This section proved to be one of the toughest, and most enjoyable/rewarding coding endeavors I had undertaken during my studies.  Up to this point in my career, I had used Java on a number of occasions, though I was far from proficient with it.
</p>
<!-- The foundation of any search engine is the index on which it operates, or stated another way: a search engine is only as good as its index (disregarding more advanced topics like query parsing). -->
Before launching into technical details of the MapReduce code, I think it's worthwhile to cover the concept of Term Frequency-inverse Document Frequency (TF-iDF).  TF-iDF is the basis for our index's scoring metric, and by extension the search engine's ranking system.  In absence of scoring/ranking a search engine will default to returning *any and all* results that contain the query terms (i.e. a boolean search).  Such a system may be difficult to work with when user's queries include common terms such as "and" or "the".

## TF-iDF

In search of a better system, we settled on a weighting (or scoring) system know as Term Frequency--Inverse Document Frequence, more commonly known by its initialism:  $$\text{tf-idf}$$.  This scoring system produces a separate score for each word $k$ in every tweet $$t$$.

The values for $\text{tf-idf}$ posses the following properties:
1. they are the greatest when $k$ occurs many times within a small number of tweets
2. lower when $k$ either  occurs fewer times in a tweet, or occurs in many tweets
3. lowest when the term occurs in virtually all tweets.

So given a query $Q$ composed of words $q\in Q$, the relavency score for every tweet is calculated as:

$$\text{Score}(q,t) = \sum_{k\in q}{\text{tf-idf}_{k,t}}$$

Where:

$$\text{tf-idf}_{k,t} = \text{tf}_k \times \text{idf}_k$$

#### Term Frequency: $$\text{tf}_k$$

Term Frequency, or $$\text{tf}_k$$, is simply the ratio of the number of times term $k$ occurs in a tweet to the total number of terms in a tweet, or mathematically stated:

$$\text{tf}_k=\frac{f_k}{\sum_{j=1}^{t}{f_j}}$$

Where $$f_k$$ is the frequency of term $$k$$ in a tweet and the summation in the denominator is simply the total number of terms in said tweet.

#### Inverse Document Frequency: $$\text{idf}_k$$
The Inverse Document Frequency is best described in the following quote:

> The inverse document frequency component ($IDF$) reflects the importance of the term in the collection of documents. The more documents that a term occurs in, the less discriminating the term is between documents and, consequently, the less useful it will be in retrieval.
> -- <cite><a href="https://ciir.cs.umass.edu/irbook/">Search Engines: Information Retrieval in Practice</a></cite>

As such, suppose we have $$N$$ tweets in our collection, of which $$n_k$$ tweets contain term $k$, then the  inverse document frequency $$(\text{idf}_k)$$ is:

$$\text{idf}_k = \log{\left(\frac{N}{n_k}\right)}$$

Note the presence of $\log$ in the function.  The purpose of the logarthim is to modulate the effect of frequently occuring terms that may exist in a large number of tweets (e.g. *"the"*, *"me"*, *"and"*).  By taking the lograthim of $N/n_k$, we can attenuate the final score directly as a function of a query term's frequency.

<img class="centerimg" src="/img/project1/IDF.webp">

The plot above shows us that as $n_k \rightarrow N$ (i.e. term $k$ can be found in almost every tweet), the term's score is reduced to zero.



## Nitty-Gritty (coding)

The code can be found in the following link:
<div style="text-align:center; width=768px;">
  <a href="https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/mrPhase_Final.java">
    <input  type="button"
            class="bigButton"
            value="TF-IDF MapReduce Code (GitHub)"
            href="https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/mrPhase_Final.java"/>
  </a>
</div>

<!-- <img src="/assets/images/meta/GitHub-Logo.png"> -->
<!-- <a href="https://github.com/adik0861/adik0861.github.io/blob/master/assets/code/mr/mrPhase_Final.java">
<div class="bigButton" style="margin-left:auto; margin-right:auto;" >
    TF-IDF MapReduce Code (GitHub)
</div>
</a> -->

The goal of this portion of the project was to convert the Twitter dataset into an index of the form:

<table class="tableizer-table"  >
   <thead>
      <tr class="tableizer-firstrow">
         <th> Term $k$ </th>
         <th> Unique ID </th>
         <th> Doc Freq. $df_k$ </th>
         <th> Term Freq. $f_k$ </th>
         <th> Score $\text{TF-iDF}$ </th>
      </tr>
   </thead>
   <tbody>
      <tr>
         <td>facebook</td>
         <td>10949481939162</td>
         <td> 7/10000</td>
         <td> 1/24</td>
         <td>0.131454248</td>
      </tr>
      <tr>
         <td>facial  </td>
         <td>109425482036444</td>
         <td>  1/10000</td>
         <td> 1/18</td>
         <td>0.222222222</td>
      </tr>
   </tbody>
</table>

To achieve this using MapReduce required that three distinct phases as well as a small hack to keep a tally of total document count.  An outline of the MapReduce job is shown below.  The next couple sections will cover the MapReduce code step by step.

<!-- <img class="centerimg" src="/img/project1/mapreduce.png"> -->
<img class="centerimg" src="/img/project1/mapreduce.webp">


## Prelude
The code begins with the following steps:
1. It defines a list of the most [common stop words](https://www.ranks.nl/stopwords)--i.e. words like "an" or "the".
2. It instantiates a [Snowball stemmer](http://snowball.tartarus.org/compiler/snowman.html) (a stemmer is a [crude heuristic process](https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html) that transforms words such as "running" into "run").
3. It initializes a  *counter* to keep track of the total tweets processed.  This is done so that different indexes can be built for various cuts of the CSV file (e.g. only index tweet's that contain geolocation data)

<!-- <details><summary><span class='fold'>Click Here to Expand The Code</span></summary><div markdown="1"> -->
```java
// hashmap to get a total count of docs (used in phase 3 to calculate iDF)
private static HashMap<String, Integer> outputHash = new HashMap<>();
// define stopwords here (avoid repeatedly creating this same list later)
private static String[] stopWords = new String[]{"a","an","and","are","as","at","be","but","by","for","if","in","into","is","it","no","not","of","on","or","such","that","the","their","then","there","these","they","this","to","was","will","with"};
private static Set<String> stopWordsDict = new HashSet<>(Arrays.asList(stopWords));
// instantiate a stemmer class here once only
private static SnowballStemmer sbStemmer = new SnowballStemmer(ENGLISH, 1);
// define custom counter here to keep track of total doc count for each index
private static enum indexCounter
{
  AllTweets, AllTweetsHashOnly, OnlyTweetsWithGeoHashOnly, OnlyTweetsWithGeo
}
```
<!-- </div></details> -->

### Phase 1: Mapper<span style="color:LightGray">-Reducer</span>
The class `mapper1` does a few different things:
1. It begins with reading the CSV file contain the tweets and extracts the tweet text as well as tweet ID
2. The tweet text is then cleaned up using "regExReplace" function and tokenized.
3. The tokens are looped through and stemmed if applicable (i.e. hastags are not stemmed).
The final output will contain a key composed of the term and docID separated by a tilde and value of one (e.g. `docID~1`).

<details>
<summary><span class='fold'>Click Here to Expand The Code</span></summary>
<div markdown="1">
```java
public static class mapper1
  extends Mapper<LongWritable, Text, Text, IntWritable>
{
  private Text wordDocPair = new Text();
  private IntWritable one = new IntWritable(1);

  private String regExReplace(String textStr)
  {
    textStr = textStr.toLowerCase();
    // Remove URLS
    textStr = textStr.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "");
    // Remove single dashes
    textStr = textStr.replaceAll("([^-])([-])([^-])", "$1$3");
    // Replace any non-Alphanumeric repeating character with single instance
    textStr = textStr.replaceAll("(\\W)\\1+", "$1");
    // Replace contraction of #'s with "numbers"
    textStr = textStr.replaceAll("(#'s)\\s", "numbers ");
    // Ensure that all #/@ have a space before them to ensure tokenization
    textStr = textStr.replaceAll("([^\\s])([#]\\w+)", "$1 $2");
    // Remove @user mentions and any non alphanumeric characters (excluding #)
    textStr = textStr.replaceAll("[@]\\w+|[@]\\W+|[^\\w#\\s]", " ");
    // Remove dashes, single characters, and useless hashtags (e.g. #1)
    textStr = textStr.replaceAll("(^| ).(( ).)*( |$)", "$1");
    textStr = textStr.replaceAll("[#][\\w\\W]\\s", "");
    // Remove repeated spaces
    textStr = textStr.replaceAll("\\s+", " ").trim();
    return textStr;
  }

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    String param = conf.get("indexType");
    String entireTweetString = value.toString();
    String[] entireTweetArray = entireTweetString.split(",");
    String tweetID = entireTweetArray[21];
    //  9 = rawtext, since it includes #tags
    String tweetText = regExReplace(entireTweetArray[9]);
    String geolocation = entireTweetArray[19].trim();

    if ((param.equals("OnlyTweetsWithGeo") || param.equals("OnlyTweetsWithGeoHashOnly")) && geolocation.length() > 8)
    {
      // Count TOTAL number of documents for each of the four criteria
      // This is used in Phase 3 below to calculate inverse doc frequency LOG(N/n)
      if (param.equals("OnlyTweetsWithGeoHashOnly") && tweetText.contains("#"))
      {
        context.getCounter("indexCounter", param).increment(1);
      }
      if (param.equals("OnlyTweetsWithGeo"))
      {
        context.getCounter("indexCounter", param).increment(1);
      }

      StringTokenizer itr = new StringTokenizer(tweetText, " ");
      while (itr.hasMoreTokens())
      {
        String strToken = itr.nextToken().trim();
        // check the type of index that's being built and that the tokens meet the indexes specific conditions
        if (((param.equals("OnlyTweetsWithGeo")) && (stopWordsDict.contains(strToken)))
            || ((param.equals("OnlyTweetsWithGeoHashOnly")) && (!strToken.substring(0, 1).equals("#"))))
        {
          continue;
        }
        CharSequence csToken = strToken;
        // exclude calls to the stemmer for hashtag tokens
        if (!strToken.substring(0, 1).equals("#"))
        {
          csToken = sbStemmer.stem(csToken);
        }
        wordDocPair.set(csToken + "~" + tweetID);
        context.write(wordDocPair, one);
      }
    }

    if (param.equals("AllTweets") || param.equals("AllTweetsHashOnly"))
    {
      // Count TOTAL number of documents for each of the four criteria
      // This is used in Phase 3 below to calculate inverse doc frequency LOG(N/n)criteria
      if (param.equals("AllTweetsHashOnly") && tweetText.contains("#"))
      {
        context.getCounter("indexCounter", param).increment(1);
      }
      if (param.equals("AllTweets"))
      {
        context.getCounter("indexCounter", param).increment(1);
      }

      StringTokenizer itr = new StringTokenizer(tweetText, " ");
      while (itr.hasMoreTokens())
      {
        String strToken = itr.nextToken().trim();
        // The following checks the type of index that's being built and that the tokens
        // meet the indexes specific conditions
        if (((param.equals("AllTweets")) && (stopWordsDict.contains(strToken)))
            || ((param.equals("AllTweetsHashOnly")) && (!strToken.substring(0, 1).equals("#"))))
        {
          continue;
        }
        CharSequence csToken = strToken;
        // exclude calls to the stemmer for hashtag tokens
        if (!strToken.substring(0, 1).equals("#"))
        {
          csToken = sbStemmer.stem(csToken);
        }
        wordDocPair.set(csToken + "~" + tweetID);
        context.write(wordDocPair, one);
      }
    }
  }
}
```
</div></details>

The output of ```mapper1``` will resemble the following:

<div class="outputTexSize">
$$
\begin{align}
  & \texttt{term}_1 \thicksim \texttt{tweet}_1 \\
  & \texttt{term}_2 \thicksim \texttt{tweet}_1 \\
  & \texttt{term}_3 \thicksim \texttt{tweet}_1 \\
  & \vdots \\
  & \texttt{term}_1 \thicksim \texttt{tweet}_n \\
  & \texttt{term}_2 \thicksim \texttt{tweet}_n \\
  & \texttt{term}_3 \thicksim \texttt{tweet}_n \\
\end{align}
$$
</div>

### Phase 1:  <span style="color:LightGray">Mapper-</span>Reducer

Next up, the associated reducer class simply counts/sums up all the incoming ```term~docID``` keys.
<!-- <details><summary><span class='fold'>Click Here to Expand The Code</span></summary><div markdown="1"> -->
```java
public static class reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>
{
  private IntWritable occurrencesOfWord = new IntWritable();

  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException
  {
    int sum = 0;
    for (IntWritable val : values)
    {
      sum += val.get();
    }
    occurrencesOfWord.set(sum);
    context.write(key, occurrencesOfWord);
  }
}
```
<!-- </div></details> -->
Since these key-value pairs are all unique, ```reducer1``` outputs a key-value pair of the form:

<div class="outputTexSize">
$$
\begin{align}
  (& \texttt{term}_1 \thicksim \texttt{tweet}_1, 1 ) \\
  (& \texttt{term}_2 \thicksim \texttt{tweet}_1, 1 ) \\
  (& \texttt{term}_3 \thicksim \texttt{tweet}_1, 1 ) \\
  & \vdots \\
  (& \texttt{term}_1 \thicksim \texttt{tweet}_n, 1 ) \\
  (& \texttt{term}_2 \thicksim \texttt{tweet}_n, 1 ) \\
  (& \texttt{term}_3 \thicksim \texttt{tweet}_n, 1 ) \\
\end{align}
$$
</div>


### Phase 2: Mapper<span style="color:LightGray">-Reducer</span>

This Phase 2 mapper, ```mapper2``` is fairly straight forward as it simply splits apart or rearranges the output from ```reducer1``` above in the following manner:

<div class="outputTexSize">
$$
\begin{equation}
\texttt{<term}\thicksim\texttt{docID,count>} \\
\downarrow \\
\texttt{<term,docID=count>}
\end{equation}
$$
</div>



```java
public static class mapper2 extends Mapper<LongWritable, Text, Text, Text>
{
  private Text docID = new Text();
  private Text termCount = new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String doc = value.toString().split("\t")[0].split("~")[1];
    String term = value.toString().split("\t")[0].split("~")[0];
    String count = value.toString().split("\t")[1];
    docID.set(doc);
    termCount.set(term + "=" + count);
    context.write(docID, termCount);
  }
}
```
Note that the count value is stil one as we haven't aggregated the values yet (that's the next step).

### Phase 2: <span style="color:LightGray">Mapper-</span>Reducer

This step is a little tricky as it's actually doing two aggregations.  The full reducer code is shown below, though the magic happens in the two nested ```for``` loops within ```reducer2```.

```java
public static class reducer2 extends Reducer<Text, Text, Text, Text>
{
  private Text termDocPair = new Text();
  private Text termFreq = new Text();
  protected void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
  {
    int countTermsInDoc = 0;
    Map<String, Integer> dict = new HashMap<>();
    for (Text val : values)
    {
      String term = val.toString().split("=")[0];
      String termCount = val.toString().split("=")[1];
      dict.put(term, Integer.valueOf(termCount));
      countTermsInDoc += Integer.parseInt(termCount);
    }
    for (String dictKey : dict.keySet())
    {
      termDocPair.set(dictKey + '~' + key.toString());
      termFreq.set(dict.get(dictKey) + "/" + countTermsInDoc);
      context.write(termDocPair, termFreq);
    }
  }
}
```

#### First ```for``` Loop
To begin with, note the instantiation of the (poorly named) map ```dict```:

```java
Map<String, Integer> dict = new HashMap<>();
```

```dict``` is where we store our initial summation in the form of a key-value pair.  The following loop simply calculates the number of times a term occurs in each tweet.

```java
Map<String, Integer> dict = new HashMap<>();
for (Text val : values)
{
  String term = val.toString().split("=")[0];
  String termCount = val.toString().split("=")[1];
  dict.put(term, Integer.valueOf(termCount));
  countTermsInDoc += Integer.parseInt(termCount);
}
```

So the output of this initial loop will look like:

<div class="outputTexSize">
$$\texttt{<term}\thicksim\texttt{docID,termCount>}$$
</div>

#### Second ```for``` Loop
The output from the first ```for``` loop above is then looped through in the next ```for``` loop (shown below).  This second loop utilizes the ```countTermsInDoc``` value from above to calculate the $\texttt{termFrequency}$ of each term within a tweet.

```java
for (String dictKey : dict.keySet())
{
  termDocPair.set(dictKey + '~' + key.toString());
  termFreq.set(dict.get(dictKey) + "/" + countTermsInDoc);
  context.write(termDocPair, termFreq);
}
```

The final output of all this (i.e. ```reducer2```) is of the form:
<div class="outputTexSize">
$\1$
</div>

Note that ***the forward slash shown above in the right hand side is actually a placeholder***.  In fact the entire right-hand side of the key-value pair is output as a string.  We're not interested in reducing the value down to a float as we end up losing information (e.g. $500/1000$ is not the same as $1/2$ for our purposes).

### Phase 3: Mapper<span style="color:LightGray">-Reducer</span>

```java
public static class mapper3 extends Mapper<LongWritable, Text, Text, Text>
{
  private Text term = new Text();
  private Text docTermFreqPair = new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String docID = value.toString().split("\t")[0].split("~")[1];
    String termStr = value.toString().split("\t")[0].split("~")[0];
    String termFreq = value.toString().split("\t")[1];
    term.set(termStr);
    docTermFreqPair.set(docID + "=" + termFreq);
    context.write(term, docTermFreqPair);
  }
}
```
Similar to ```mapper2``` previously, ```mapper3``` below simply rearranges the output of ```reducer2``` in the following form:

<div class="outputTexSize">
$$
\begin{equation}
\texttt{<term}\thicksim\texttt{docID,termCount/countOfTermsInDoc>} \\
\downarrow \\
\texttt{<term,docID=termCount/countOfTermsInDoc>}
\end{equation}
$$
</div>



And as mentioned previously, the ***forward slash isn't a division sign, and similarly the equals sign isn't an assignment operator***.  Both are simply placeholders to keep all the values separate.


### Phase 3: <span style="color:LightGray">Mapper-</span>Reducer

This next section, while looking somewhat complex, is actually fairly straight forward.  The output of ```mapper3``` above is used to finally calculate the $tf-idf$ score and the resulting output of this final reducer class is in the form:

<div class="outputTexSize">
$$
\begin{equation}
\texttt{<term,docID=termCount/countOfTermsInDoc>} \\
\downarrow \\
\texttt{<term}\thicksim\texttt{docID, df, tf, tf-idf>} \\
\end{equation}
$$
</div>

Where $$\texttt{df}$$ is the document frequency and $$\texttt{tf}$$ is the term frequency.




<details><summary><span class='fold'>Click Here to Expand The Code</span></summary><div markdown="1">
```java
public static class reducer3 extends Reducer<Text, Text, Text, Text>
{
  private Text docTerm = new Text();
  private Text valStr = new Text();
  protected void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    int totalCountOfDocs = Integer.valueOf(conf.get("docCount"));
    int countOfDocsWithTerm = 0;
    Map<String, String> dict = new HashMap<>();
    for (Text val : values)
    {
      String docID = val.toString().split("=")[0];
      String termFreq = val.toString().split("=")[1];
      dict.put(docID, termFreq);
      countOfDocsWithTerm++;
    }
    for (String document : dict.keySet())
    {
      double numerator = Double.valueOf(dict.get(document).split("/")[0]);  // LHS of operand
      double denominator = Double.valueOf(dict.get(document).split("/")[1]);  // RHS of operand
      double TF = numerator / denominator;
      double iDF = (double) totalCountOfDocs / (double) countOfDocsWithTerm;
      // if doc freq = 1 then only use term-freq as Log(iDF) = Log(1) = 0
      double TFiDF = iDF == 1 ? TF : TF * Math.log10(iDF);
      docTerm.set(key + "~" + document);
      String strDocFreq = countOfDocsWithTerm + "/" + totalCountOfDocs;
      String strTermFreq = (int) numerator + "/" + (int) denominator;
      String StrTFiDF = String.format("%.10f", TFiDF);
      String StrConcat = "[" + strDocFreq + "," + strTermFreq + "," + StrTFiDF + "]";
      valStr.set(StrConcat);
      context.write(docTerm, valStr);
    }
  }
}
```
</div></details>
