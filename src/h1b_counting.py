import sys

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
                values.append(raw_list[i])
                i = i + 1
            else:
                j = i + 1
                entry = raw_list[i]
                while j < len(raw_list) and raw_list[j][-1] != '\"':
                    entry = entry + raw_list[j]
                    j = j + 1
                if j == len(raw_list):
                    i = j
                else:
                    entry = entry + raw_list[j]
                    values.append(entry)
                    i = j + 1
                    
    return values
                

def create_state_fullname_dict(filename = './src/state_fullname_dictionary.txt'):
    file = open(filename, "r")

    state_fullname_dict = {}
    for line in file:
        entry = tuple(map(lambda s: s.strip(), line.split(',')))
        state_abb = entry[0]
        state_full = entry[1]
        if state_fullname_dict.get(state_abb) == None:
            state_fullname_dict[state_abb] = state_full

    file.close()
    return state_fullname_dict

def row_to_dict(fields, values):
    return dict(zip(fields, values))

def h1b_count(src_path, dst_path_occupation, dst_path_state):
    in_file = open(src_path, "r")
    print("Start to process file '{filename}'".format(filename = src_path))

    occupation_dict = {}
    state_dict = {}
    state_name_dict = create_state_fullname_dict()
    
    head = in_file.readline()
    fields = ['INDEX'] + head[:-1].split(';')[1:]
    
    num_obs = 0
    line_cnt = 0
    for line in in_file:
        line_cnt = line_cnt + 1
        if line_cnt % 100000 == 0:
            print("Processed 100000 entries");
        values = get_values_from_line(line)
        obs = row_to_dict(fields, values)
        if obs['VISA_CLASS'] != "H-1B" or obs['CASE_STATUS'] != "CERTIFIED":
            continue
        num_obs = num_obs + 1

        occupation = obs['SOC_NAME']
        if occupation_dict.get(occupation) == None:
            occupation_dict[occupation] = 1
        else:
            occupation_dict[occupation] = occupation_dict[occupation] + 1
            
        state = obs['WORKSITE_STATE']
        if state_dict.get(state) == None:
            state_dict[state] = 1
        else:
            state_dict[state] = state_dict[state] + 1

    print("Finished processing file '{filename}'".format(filename = src_path))

    in_file.close()

    print("Total number of certified H1B cases:", num_obs)
    
    # For Occupation Stats
    top_occupation = [(k, occupation_dict[k]) for k in sorted(occupation_dict, key = occupation_dict.get, reverse = True)][0:10]

    out_file_occupation = open(dst_path_occupation, "w")
    # Print head info.
    out_file_occupation.write("TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    for (k, v) in top_occupation:
        out_file_occupation.write("{0};{1};{2:.1f}%\n".format(k, v, v/num_obs*100))
    out_file_occupation.close()

    # For State Stats
    top_state = [(k, state_dict[k]) for k in sorted(state_dict, key = state_dict.get, reverse = True)][0:10]
    out_file_state = open(dst_path_state, "w")
    # Print head info.
    out_file_state.write("TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    for (k, v) in top_state:
        state_name = "";
        if state_name_dict.get(k) != None:
            state_name = state_name_dict[k]
        else:
            state_name = k
        out_file_state.write("{0};{1};{2:.1f}%\n".format(state_name, v, v/num_obs*100))
    out_file_state.close()

    

if __name__ == "__main__":
    src_path = sys.argv[1]
    dst_path_occupation = sys.argv[2]
    dst_path_state = sys.argv[3]

    h1b_count(src_path, dst_path_occupation, dst_path_state)
