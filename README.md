# insight-challenge
Solution to Data Engineer Bootcamp Challenge Session for Jan 2019

# Problem

Code in this repository is to solve the data challenge from insight bootcamp. Details of problem and dataset specification can be found [here](https://github.com/InsightDataScience/h1b_statistics).

# Language of Choice

My implementation is written in Python3. The `run.sh` has been modified by running `python3` followed by command-line arguments of script, input file, and output files.

It's tested on Ubuntu Linux 18.04, and successfully passed the provided `h1b_input.csv` test. If your system uses `python` instead of `python3` as the static link to Python3, please change it in `run.sh`.

# Implementation

The process is done line by line at runtime. Since the dataset is usually large, scale of 100+MB, it may be difficult for machines with small memory to hold all observations at one time.

## Header (Column names)

First line consists of column names (i.e. field names). I did two jobs:
* Split the line string by ';', and add the column name **INDEX** at the front, as there is no column name given to the first column in the dataset. Return a list of column names.
* Using the preset lists of column names of Cetified Status, Occupation, and State (since column names vary among different years' datasets), deciding the names of these three critical columns for the current dataset. Store the result as **certified_fieldname**, **occupation_fieldname**, and **state_fieldname**.
  
## Observations

For each line (i.e. observation),:
1. Split the line string by ';'. Notice that his time, I didn't use the built-in `split` function, but use my own split `get_value_from_line`. This is because the built-in `split` function considers ';' inside quote patterns. So `"xxx;yyy"` will be split as two values: `"xxx` and `yyy"`, which is incorrect. My own split function ignore this kind of ';'. The time complexity of O(n), i.e. linear.
2. Ignore observations with **certified_fieldname** value not **CERTIFIED**. Notice that for datasets not preprocessed into `".csv"` files, this value is **Certification**. So I also include the check w.r.t. this value as well.
3. Maintain a dictionary as a Hash Map structure to count the frequency of occupations. **Key** is occupation SOC name gained from **occupation_fieldname** column; **Value** is frequency. This update is O(1), i.e. constant time complexity.
4. Maintain a dictionary as a Hash Map structure to count the frequency of states. **Key** is working site state name gained from **state_fieldname** column; **Value** is frequency. This update is also O(1).

## Write H1B Stats

When done with processing the dataset, we have two dictionaries on frequency of occupations and states left. For each of the dictionary:
1. Sort the dictionary: first in descending order by frequency, then in ascending dictionary order by occupation/state name.
2. Take the first 10 items. If total items are less than 10, take all.
3. Write to output file in the required format.

## More Details

You can either refer to the docstring of functions in `h1b_counting.py`, or directly read the in-line comments in that source file. I've written sufficiently many details therein.

## Package/Data Structure Used

I used Python `list` and `dictionary` for most of the process. Python `set` is used when taking the intersection of two lists.

Package `sys` is used to process the command-line arguments on input and output file names.

# Output

If running `run.sh` in the repository directory, two output files: `top_10_occupations.txt` and `top_10_states.txt` will be generated in `output/` folder.

If running `run_tests.sh` in `insight_testsuite` folder, each of the tests in the `insight_testsuite/tests/` folder will be executed, and compare the two results with the true countings for justification.

Besides the output written to files, there are also several intermediate output information written to screen/terminal, so that user can have a clear sense on which step the machine is doing. This may help them be more patient, rather than seeing nothing during the possibly minutes long processing.

# Test

My implementation passed the provided test. I also ran it with the `csv` format files of 2014, 2015, and 2016. Although it's difficult the check the correct frequencies for those years, the ranking on top 10 states is consistent with 2017 and 2018, and the ranking of top 10 occupations is also about the same. So this gives me a bit confidence about its correctness.

# Discussion

1. To speed up the processing time, one may use multi-thread way of processing the dataset. But as this may involve using non-standard Python packages and data structures, I didn't go this way.

2. For datasets earlier than 2014 (including 2014), one case can have up to 2 working sites: **LCA_CASE_WORKLOC1_STATE** (or **State_1** for ealier than 2009Efiled) and **LCA_CASE_WORKLOC2_STATE** (Or **State_2** for earlier than 2009Efiled) columns. However, due to my exploration, most of cases have **LCA_CASE_WORKLOC2_STATE** (**State_2**) column null. So I only consider **LCA_CASE_WORKLOC1_STATE** (**State_1**) as the state information onf this case. Unless the reader wants to consider a case with two locations as two separate cases in the counting, I think my current choice logically makes sense, since the first working location is the main place the person would stay.

3. To be consistent with the official summary report in 2017 and 2018, we may also want to calculate the top 10 employers list.

4. The output of top 10 states are in abbreviations. To provide a more convenient output as the offical summary report, I would think of setting up a dictionary mapping abbreviations to full names, then use it to modify the output.
