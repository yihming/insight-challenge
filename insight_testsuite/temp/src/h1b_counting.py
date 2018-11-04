import sys

certified_columns = ['CASE_STATUS', 'STATUS', 'Approval_Status']
occupation_columns = ['SOC_NAME', 'LCA_CASE_SOC_NAME', 'Occupational_Title']
state_columns = ['WORKSITE_STATE', 'LCA_CASE_WORKLOC1_STATE', 'State_1']

def get_values_from_line(line):
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

def list_to_dict(l):
    return dict([(entry, 1) for entry in l])

def h1b_count(src_path, dst_path_occupation, dst_path_state):
    in_file = open(src_path, "r")
    print("Start to process data source file")

    occupation_dict = {}
    state_dict = {}

    # Process head and get column information.
    head = in_file.readline()
    fields = ['INDEX'] + head[:-1].split(';')[1:]
    fields_set = set(fields[1:])

    certified_fieldname = list(set(certified_columns) & fields_set)[0]
    occupation_fieldname = list(set(occupation_columns) & fields_set)[0]
    state_fieldname = list(set(state_columns) & fields_set)[0]
    
    # Iterate over entries.
    num_obs = 0
    line_cnt = 0
    for line in in_file:
        line_cnt = line_cnt + 1
        if line_cnt % 100000 == 0:
            print("Processed 100000 entries");
        values = get_values_from_line(line)
        obs = dict(zip(fields, values))
        if obs[certified_fieldname] != "CERTIFIED" and obs[certified_fieldname] != "Certification":
            continue
        num_obs = num_obs + 1

        occupation = obs[occupation_fieldname]
        if occupation_dict.get(occupation) == None:
            occupation_dict[occupation] = 1
        else:
            occupation_dict[occupation] = occupation_dict[occupation] + 1
            
        state = obs[state_fieldname]
        if state_dict.get(state) == None:
            state_dict[state] = 1
        else:
            state_dict[state] = state_dict[state] + 1

    print("Finished processing data source file")

    in_file.close()

    
    # For Occupation Stats
    tmp_list = sorted(occupation_dict.items(), key = lambda entry : entry[0])
    top_occupation = sorted(tmp_list, key = lambda entry : entry[1], reverse = True)[0:10]

    out_file_occupation = open(dst_path_occupation, "w")
    # Print head info.
    out_file_occupation.write("TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    # Print entries.
    for (k, v) in top_occupation:
        out_file_occupation.write("{0};{1};{2:.1f}%\n".format(k, v, v/num_obs*100))
    out_file_occupation.close()

    # For State Stats
    tmp_list = sorted(state_dict.items(), key = lambda entry : entry[0])
    top_state = sorted(tmp_list, key = lambda entry : entry[1], reverse = True)[0:10]

    out_file_state = open(dst_path_state, "w")
    # Print head info.
    out_file_state.write("TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    # Print entries.
    for (k, v) in top_state:
        out_file_state.write("{0};{1};{2:.1f}%\n".format(k, v, v/num_obs*100))
    out_file_state.close()

    

if __name__ == "__main__":
    src_path = sys.argv[1]
    dst_path_occupation = sys.argv[2]
    dst_path_state = sys.argv[3]

    h1b_count(src_path, dst_path_occupation, dst_path_state)
