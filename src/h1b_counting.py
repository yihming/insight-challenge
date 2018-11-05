import sys

# List of column names for Certified Status, Occupation, and State, across data in different years.
certified_columns = ['CASE_STATUS', 'STATUS', 'Approval_Status']
occupation_columns = ['SOC_NAME', 'LCA_CASE_SOC_NAME', 'Occupational_Title']
state_columns = ['WORKSITE_STATE', 'LCA_CASE_WORKLOC1_STATE', 'State_1']


def get_values_from_line(line):
    """Get values of an observation as a list from string,
    splitted by semicolons.
    Note 1: Ignore ';' inside \"...\". E.g. consider \"xxx;yyy\" as one value.
    Note 2: Null string '' for NA values.
    """
    raw_list = line.strip().split(';')
    i = 0
    values = []
    while i < len(raw_list):
        if raw_list[i] == "" or raw_list[i][0] != '\"':
            values.append(raw_list[i])
            i = i + 1
        else:
            if raw_list[i][-1] == '\"':
                values.append(raw_list[i][1:-1])
                i = i + 1
            else:
                j = i + 1
                entry = raw_list[i][1:]
                while j < len(raw_list) and raw_list[j][-1] != '\"':
                    entry = entry + raw_list[j]
                    j = j + 1
                if j == len(raw_list):
                    i = j
                else:
                    entry = entry + raw_list[j][:-1]
                    values.append(entry)
                    i = j + 1
                    
    return values

def h1b_count(src_path, dst_path_occupation, dst_path_state):
    """Process observations from 'src_path', write H1B stats to 'dst_path_occupation' and 'dst_path_state'.
    'dst_path_occupation' stores top 10 occupations having certified H1B cases in the year, along with frequency and percentage;
    'dst_path_state' stores top 10 states having certified HAV cases in the year, along with frequency and percentage.
    """
    
    in_file = open(src_path, "r")
    print("Start to process data source file")

    # Initialize dictionary-structure counters for occupation and state.
    occupation_dict = {}
    state_dict = {}

    # Process head and get column information.
    head = in_file.readline()
    fields = ['INDEX'] + head[:-1].split(';')[1:]
    fields_set = set(fields[1:])

    # Get column names of Certified Status, Occupation, and State by taking the intersection of
    # column names of current dataset and the corresponding keyword column name list.
    certified_fieldname = list(set(certified_columns) & fields_set)[0]
    occupation_fieldname = list(set(occupation_columns) & fields_set)[0]
    state_fieldname = list(set(state_columns) & fields_set)[0]
    
    # Iterate over observations. #
    # Counter on certified H1B cases.
    num_obs = 0
    # Counter on observations processed.
    line_cnt = 0
    for line in in_file:

        # Print to screen as real-time feedback to user.
        line_cnt = line_cnt + 1
        if line_cnt % 100000 == 0:
            print("Processed 100000 entries");

        # Process each observation; stored as a dictionary for constant time visit.
        values = get_values_from_line(line)
        obs = dict(zip(fields, values))

        # Ignore uncertified cases.
        if obs[certified_fieldname] != "CERTIFIED" and obs[certified_fieldname] != "Certification":
            continue

        # Update certified H1B case counter.
        num_obs = num_obs + 1

        # Update occupation frequency counter structure.
        occupation = obs[occupation_fieldname]
        if occupation_dict.get(occupation) == None:
            occupation_dict[occupation] = 1
        else:
            occupation_dict[occupation] = occupation_dict[occupation] + 1

        # Update state frequency counter structure.
        state = obs[state_fieldname]
        if state_dict.get(state) == None:
            state_dict[state] = 1
        else:
            state_dict[state] = state_dict[state] + 1

    # Finish processing input file.
    print("Finished processing data source file")
    in_file.close()

    
    # Write top 10 occupations to file. #
    # Sort the occupation frequency counter.
    # Main order: descending order by frequency, i.e. second element of (key, value) tuple.
    # Secondary order: ascending order by dictionary order of string, i.e. first element of (key, value) tuple.
    # Finally take the first 10 occupations. Take all if total occupations appearing are less than 10.
    tmp_list = sorted(occupation_dict.items(), key = lambda entry : entry[0])
    top_occupation = sorted(tmp_list, key = lambda entry : entry[1], reverse = True)[0:10]

    # Open the file to write.
    out_file_occupation = open(dst_path_occupation, "w")
    # Print head info.
    out_file_occupation.write("TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    # Print entries.
    for (k, v) in top_occupation:
        out_file_occupation.write("{0};{1};{2:.1f}%\n".format(k, v, v/num_obs*100))
    # Close the write file.
    out_file_occupation.close()

    # Write top 10 states to file. #
    # Similarly as occupation counter, sort the occupation frequency counter.
    # Main order: descending order by frequency, i.e. second element of (key, value) tuple.
    # Secondary order: ascending order by dictionary order of string, i.e. first element of (key, value) tuple.
    # Finally take the first 10 states. Take all if total states appearing are less than 10.
    tmp_list = sorted(state_dict.items(), key = lambda entry : entry[0])
    top_state = sorted(tmp_list, key = lambda entry : entry[1], reverse = True)[0:10]

    # Open the file to write.
    out_file_state = open(dst_path_state, "w")
    # Print head info.
    out_file_state.write("TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    # Print entries.
    for (k, v) in top_state:
        out_file_state.write("{0};{1};{2:.1f}%\n".format(k, v, v/num_obs*100))
    # Close the write file.
    out_file_state.close()

    

if __name__ == "__main__":
    # Get input and two output file paths from command line.
    src_path = sys.argv[1]
    dst_path_occupation = sys.argv[2]
    dst_path_state = sys.argv[3]

    h1b_count(src_path, dst_path_occupation, dst_path_state)
