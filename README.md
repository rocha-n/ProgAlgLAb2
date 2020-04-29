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

## Task 2 - Finding the N most frequent k-mers

## Task 3 - Finding the longest unique k-mer

## Results

## Conclusion
