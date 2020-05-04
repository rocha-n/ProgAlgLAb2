# ProgAlg - Lab2 
K-mer Frequency  
_N. Rocha, K. Amrani_

## Introduction
This is the report of a lab done in the context of a HES-SO Engineering Master curriculum.  
As it is on the subject of biology, here are some notions worth knowing for its understanding. 

First, a _genome_ is a complete set of DNA (or RNA for some viruses), containing all the information describing a biological organism. 

The base unit of a genome is the _nucleotide_. In the case of DNA, there are four of them:
- Adenine  
- Cytosine  
- Guanine  
- Thymine  

The nucleotides are chained together, and the resulting sequence is called a _polymer_.

A _k-mer_ is a polymer subset of length _k_. 
So, for a given polymer, we can search for _monomers_ (when k = 1), _2-mers_, _3-mers_, and so on until _k_ equals the length of the polymer.  

The interest of doing this operation is that the frequency of certain k-mer probes can be used to identify the species described by the genome.  

## Scope of the lab
In this lab, we will do several operations on a digital representation of a genome, which will be a plain text file.  
Structure-wise, it will contain a first line with genome-related information, and then each line is a polymer of length 70.  
Our file contains the genome of an E. coli bacteria, and looks like this:
```
>gi|49175990|ref|NC_000913.2| Escherichia coli K12 substr. MG1655, complete genome
AGCTTTTCATTCTGACTGCAACGGGCAATATGTCTCTGTGTGGATTAAAAAAAGAGTGTCTGATAGCAGC
TTCTGAACTGGTTACCTGCCGTGAGTAAATTAAAATTTTATTGACTTAGGTCACTAAATACTTTAACCAA
```

Using this data, three tasks have been defined:
1. Count the number of identical k-mers 
2. Find the N most frequent k-mers
3. Find the longest unique k-mer

The constraints are:  
- For each of the tasks, the use of Hadoop will be mandatory.   
- For each of the tasks, the _k_ value (for the _k_-mers) will be provided by argument when calling the function.  
- For the second task, the _N_ value will be provided by argument when calling the function.  
- For each of the tasks, a description of its workings and the syntax of its command line will be given in this report. 
- This report as well as the whole code, input and output files will be provided. 

We won't cover:  
- Hadoop installation/configuration. 
- Hadoop basics. 
- No testing with another input source than the E. coli genome described above. 

## Task 1 - Counting the number of identical k-mers
The first task is to parse the file in order to list the number of occurrences of each encoutered k-mer, for a given k.
As stated before, the first line of the file is a header, so we have to get rid of it in our calculations.

Also, we have to take in account the k-mers that would have started on a line and ended on following one.
To demonstrate this, let's take the following 2-line file as input:
```
ACGG
TACT
```

For k = 2, the expected output would be:
```
AC	2
CG	1
CT	1
GG	1
GT	1
TA	1
```

In this case, the `GT` occurence found is composed of the last character of the first line and the first of the second line.
To solve this issue, we have created a first job, called _line combiner_, that will parse the input and append the needed number of characters at the end of each line to prevent this k-mer on two lines problem, and output the file to a _temp_ folder.

The _line combiner_ mapper uses the data file offset provided by the key parameter to know on which line we are, and constructs two outputs for each line:
- one with the previous line offset as key, and _k_-1 characters to send to the line above, preceded by the `<` symbol as value
- one with the current line offset as key, and the input line followed by the symbol `>` as value

Here is an excerpt of the mapper, where this operation occurs:
```
  LongWritable start = new LongWritable(((LongWritable) key).get());// key for previous line
  LongWritable end = new LongWritable(start.get() + input.length() + 1);// key for current line

  word.set("<" + input.substring(0, Math.min(k - 1, input.length())));// give k-1 first letters to previous line
  context.write(start, word);

  word.set(input + ">");
  context.write(end, word);
```                

The _line combiner_ reducer will then take advantage of the two symbols to know if a value for a given key is at the start or at the end of a string, and construct its output.

Here is the reduce method, as implemented in the solution:
```
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String start = null;// current line
            String end = null;// start of next line
            for (Text val : values) {
                String str = val.toString();
                if (str.charAt(0) == '<') end = str;
                else start = str;
            }
            if (start != null && end != null) {
                result.set(start.substring(0, start.length() - 1) + end.substring(1, end.length()));
                context.write(key, result);
            } else if (start != null && end == null) {// if no next line don't try to append
                result.set(start.substring(0, start.length() - 1));
                context.write(key, result);
            }
        }
```

In the case described above the output file would be:
```
5 ACGGT
10 TACT
```

As we can see, after the _line combiner_ completes, the `GT` k-mer is now on a single line.

The second job, named _k-mer counter_, will start by checking that the input starts by one of the allowed characters (A, C, G, or T) before proceeding. If it's not the case, the line might be the header of the file or a bugus line, and so will be discarded.

Otherwise, the job will extract each substring of the _temp_ file created by the _line combiner_ of length _k_, and send it to the reducer with value 1. The reducer will then sum the values, and display the key with its total to the output folder, thus producing the expected result.

To launch this first task on hadoop, use the following command:
`$ hadoop jar ProgAlg1.jar Lab2Task1 <k> <input folder> <output folder>`

For example, to start the job for 7-mers with input file on `input` folder and results in `output` folder:
`$ hadoop jar ProgAlg1.jar Lab2Task1 7 input output>`

Note: Make sure you don't already have a temp folder on hdfs, otherwise the program will throw an exception because it hasn't the rights to overwrite the existing one. To delete it, `$ hdfs dfs -rmdir temp`

## Task 2 - Finding the N most frequent k-mers
This second task has for goal to list the top-_N_ k-mers in the input files, the number _N_ being provided by the user.

With the first task, we already have a list of each key and the number of its appearances in the input data.
There is just one problem left: it's ordered by key, and not by value.

It's an easy one to sort out. For example, for _N_=10, one could simply go for a bash command, in the style of:
`hdfs dfs -cat /output/part* | sort -n -k2 -r | head -n10`

But that would violate the first rule of this lab, that enforces the use of Hadoop for any task.
So we searched for an existing pattern, or at least good practice, to do the reverse sorting and limit the output to N rows.

We found a solution that would drive us to use our existing jobs for task one, and modify the way the reducer of the second job outputs its data.

So, to solve this task, we built on what had been done for previous task.
We still have two jobs. The first one, the _line combiner_, will be reused as is, without any modification.

Now, for the second job, the mapper, called _KmerCountMapper_, can also be reused as is. 
We have done modifications in the reducer.

The idea is to use a `HashMap`, that will store a pair `<Text, Integer>`, representing the key and the aggregated count of its occurences. Each reducer will do the sum of the values per key, and store this information in tht list.

Then, we called a method named `cleanup`, that gets executed only once per job, after the reducers have finished their work.
In this method, we will get the HashMap, sort it and get the _N_ results we want.

Here is the code of the cleanup task:
```
public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));

            List<Map.Entry<Text, Integer>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.<Text, Integer>comparingByValue().reversed())
                    .collect(Collectors.toList());

            int counter = 0;
            for (Map.Entry<Text, Integer> entry : sortedList) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
                if (++counter >= n) break;
            }

        }
```

Note: As mentionned in the source file, this code has been inspired [from a GitHub repo of Mr. Andrea Iacano](https://github.com/andreaiacono/MapReduce/blob/master/src/main/java/samples/topn/TopN.java).  

To launch this first task on hadoop, use the following command:
`$ hadoop jar ProgAlg1.jar Lab2Task1 <k> <n> <input folder> <output folder>`

For example, to start the job for having the top 20 8-mers with input file on `input` folder and results in `output` folder:
`$ hadoop jar ProgAlg1.jar Lab2Task2 8 20 input output>`

As for the previous task, be careful not to already have a folder named `temp` or a folder with the same name specified for output. Each of those has to be non-existent when the program is launched

## Task 3 - Finding the longest unique k-mer
For this last task, we have to find the longest unique k-mer in the input files.
Again, we start with what has been built for the previous tasks, and modify it to suit our needs.

We will still have two jobs. The _line combiner_ has still to be used, otherwise we might miss values on two lines.
But this time, we want to know not only the number of occurencies, but also the position of the string.
The goal is have a list with the position _as well_ as the number of times a given k-mer appears.
That way, we will be able to look for the strings that only appear once. After what we can check if there are identic substrings among those, enabling us to get a bigger unique k-mer.

So the first task is to have a list of k-mers, with their positions in the input file.
The mapper of the _line combiner_ can remain as is.
In the reducer, we added a `HashMap`, to which pairs of <offset,k-mer> are written.

Once the mappers have finished working, a `cleanup` method will be triggered.
Its task is to create a collection of k-mers ordered by offset.
The output will be a pair containing line number as key, and the k-mer as value.
Here is the _line combiner_ reducer `cleanup` implementation:
```
public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Long, Text>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            int lineId = 0;
            for (Map.Entry<Long, Text> entry : sortedList) {
                context.write(new LongWritable(lineId++), entry.getValue());
            }
        }
```
Obviously, the strings of the input would be modified to include the next n-1 characters of the next line, just as before.

So, if we start this job on the two-line example file defined in task one, we would obtain the following output:
```
0 ACGGT
1 TACT
```

Then, the second job will use this output to find the longuest sequence in the data.
The mapper will compute, for each line, each substring of length _k_, and its exact offset.
It's these informations that it will pass the the reducer.
Here is the code that does that calculation, and writes the key/value pairs:
```
for (int i = 0; i < subStringNb; i++){
    String kmer = input.substring(i,k+i);

    int pos = subStringNb * offset + i;
    word.set(kmer);
    context.write(word, new IntWritable(pos));
```

With this information, the reducer will filter the non-unique pairs, and write them in a `HashMap` of type `<Integer, Text>`, the key being its offset, and the value the k-mer.

Then, there is the reducer `cleanup` method that will check if there is a way to enhance the legth of the remaining k-mers, in case there is any chance to append fractions of them.
Here is the code of the reducer's `cleanup` method:
```
public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            List<Map.Entry<Integer, Text>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            int counter = 0;
            int previous = -1;
            int startOfCurrent = -1;
            int startOfBest = -1;
            int counterOfBest = -1;
            for (Map.Entry<Integer, Text> entry : sortedList) {
                int current = entry.getKey();
                if (current - previous == 1) {
                    counter++;
                } else {// if end of chain
                    if (startOfBest == -1) {// if not initialized
                        startOfBest = current;
                        counterOfBest = counter;
                    } else if (counter > counterOfBest) {// if initialized and better (bigger)
                        startOfBest = startOfCurrent;
                        counterOfBest = counter;
                    }
                    // reset
                    counter = 0;
                    startOfCurrent = current;
                }
                previous = current;
            }
            
            StringBuilder sb = new StringBuilder(countMap.get(startOfBest).toString());
            for (int i = 1; i <= counterOfBest; i++) {
                sb.append((char)(countMap.get(startOfBest + i).charAt(k - 1)));
            }
            context.write(new IntWritable(counterOfBest + k), new Text(sb.toString()));
        }
```

To launch this first task on hadoop, use the following command:
`$ hadoop jar ProgAlg1.jar Lab2Task3 <k> <input folder> <output folder>`

For example, to start the job for finding the largest unique 8-mer or more with input file on `input` folder and results in `output` folder:
`$ hadoop jar ProgAlg1.jar Lab2Task2 8 input output>`

Note: as always, make sure there is no `temp` nor `output` folder, for the above example.

## Results
### Task 1
As this task's results were pretty big, here is an excerpt of the 25 first lines for 7-mers:
```
AAAAAAA	656
AAAAAAC	652
AAAAAAG	761
AAAAAAT	866
AAAAACA	721
AAAAACC	571
AAAAACG	665
AAAAACT	553
AAAAAGA	608
AAAAAGC	744
AAAAAGG	472
AAAAAGT	447
AAAAATA	743
AAAAATC	720
AAAAATG	721
AAAAATT	602
AAAACAA	476
AAAACAC	319
AAAACAG	590
AAAACAT	426
AAAACCA	637
AAAACCC	364
AAAACCG	723
AAAACCT	448
AAAACGA	399
```

### Task 2
Here are the results for the 20 most frequents 10-mers
```
CGCATCCGGC	150
CCAGCGCCAG	144
GCATCCGGCA	143
GCCGCATCCG	139
TGCCGGATGC	136
CAGCGCCAGC	135
GCCGGATGCG	134
CTGGCGCTGG	125
GCCTGATGCG	124
CGCCGCATCC	124
GCTGGCGCTG	120
CGGATAAGGC	115
ACGCCGCATC	114
TGCCTGATGC	114
CGGATGCGGC	114
GGATGCGGCG	113
CCGCATCCGG	112
GGCGCTGGCG	109
CGCCAGCGCC	108
GCCAGCGCCA	107
```

### Task 3
Finally, this is the longuest unique k-mer, on a basis of 9-mers
```
15	AATGCCCTAGAGAGC
```
