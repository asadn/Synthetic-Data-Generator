from datagenerator.models.types import *
import json
import numpy
import re

def get_model_from_json(pattern_file):
    pattern_data = open(pattern_file).read().split("\n")
    if pattern_data[len(pattern_data)-1] == "":
        pattern_data.pop()
    model = []
    for dicts in pattern_data:
        dict_obj = eval(dicts)
        if dict_obj['col_type'] == "varchar":
            col_obj = VarcharCol(dict_obj)
        elif dict_obj['col_type'] == "float":
            col_obj = FloatCol(dict_obj)
        elif dict_obj['col_type'] == "int":
            col_obj = IntCol(dict_obj)
        elif dict_obj['col_type'] == "timestamp":
            col_obj = TimestampCol(dict_obj)
        model.append(col_obj)
    return model

class RegexDict(dict):
    def get_matching(self, event):
        return (key for key in self if re.match(key, event))
    def get_all_matching(self, events):
        return (match for event in events for match in self.get_matching(event))

def add_additional_users(model,column,count):
    # create a new entry for each user in parent column's and child column c_p_t
    # if child column is dependent of other columns, the the distribution of other column is fixed
    # Assume user column have timestamp as parent.
    # What if timestamp has multiple children
    single_parent_child = []
    multi_parent_child = []
    multi_parents = []
    for other_col in model:
        if other_col.name != column.name:
            if column.parents[0] == other_col.name:
                parent = other_col
            elif column.name in other_col.parents:
                if(len(other_col.parents) == 1):
                    single_parent_child.append(other_col)
                else:
                    multi_parent_child.append(other_col)
                    multi_parents.extend(other_col.parents)
                    multi_parents = list(set(multi_parents))
    other_users = parent.time_probs[column.name].keys()
    for col in multi_parent_child:
        multi_parents.extend(col.parents)
    multi_parents = list(set(multi_parents))
    for i in range(1,count+1):
        username = "User0"+str(i)
        dummy_user = numpy.random.choice(other_users,1)[0]
        user_timeprobs = parent.time_probs[column.name][dummy_user]
        parent.time_probs[column.name][username] = user_timeprobs
        parent.number_eventsPH[column.name][username] = parent.number_eventsPH[column.name][dummy_user]
        for child in single_parent_child + multi_parent_child:
            if (child.name not in [mc.name for mc in multi_parent_child]) and (child.name not in multi_parents):
                if child.col_type == "varchar":
                    other_parents = child.c_p_t.keys()
                    c_p_t_user = numpy.random.choice(other_parents,1)[0]
                    child.c_p_t[username] = child.c_p_t[c_p_t_user]
                elif child.col_type == "int":
                    other_parents = child.c_p_t.keys()
                    c_p_t_user = numpy.random.choice(other_parents,1)[0]
                    child.c_p_t[username] = child.c_p_t[c_p_t_user]
                    child.bandwidth[username] = child.bandwidth[c_p_t_user]
                elif child.col_type == "float":
                    other_parents = child.c_p_t.keys()
                    c_p_t_user = numpy.random.choice(other_parents,1)[0]
                    child.bandwidth[username] = child.bandwidth[c_p_t_user]
            else:
                rel_keys = []
                print("multi  "+child.name)
                for hashs in [key_val for key_val in child.c_p_t.keys() if key_val.startswith(dummy_user)]:
                    rel_keys.append(hashs)
                print len(rel_keys)
                for key_val in rel_keys:
                    c_p_t_user = ";".join( username if k == dummy_user else k for k in key_val.split(";"))
                    print c_p_t_user
                    child.c_p_t[c_p_t_user] = child.c_p_t[key_val]
                    if child.col_type == "int":
                        child.bandwidth[c_p_t_user] = child.bandwidth[key_val]
                    elif child.col_type == "float":
                        child.bandwidth[c_p_t_user] = child.bandwidth[key_val]
    return model



def main():
    pattern_file = "tests/in_data/co3_4_users.csv_model.json"

    model = get_model_from_json(pattern_file)

    header = ["username","timestamp","agent","sourceip","destination","destinationpIP","bytes_in","bytes_out", "httpResponse","method"]

    pattern_file = "tests/in_data/co3_4_users.csv_model.json"

    model = get_model_from_json(pattern_file)
    count = 100
    for cols in model:
        if cols.name == "username":
            column = cols
    new_model = add_additional_users(model,column,count)
    fw = open("new_model_model.json","w")
    for col in new_model:
        print("writing json of "+col.name)
        fw.write(str(col.__dict__) + "\n")
    fw.close()
    header = ["username","timestamp","agent","sourceip","destination","destinationpIP","bytes_in","bytes_out", "httpResponse","method"]
    tree_data_proxy = Tree(new_model, header)
    records = tree_data_proxy.generate_data(_start=datetime.datetime.strptime("2016-04-01 00:00", "%Y-%m-%d %H:%M"),
                            _end=datetime.datetime.strptime("2016-04-10 00:00", "%Y-%m-%d %H:%M"),filename="dummy.csv")


if __name__== "__main__":main()
