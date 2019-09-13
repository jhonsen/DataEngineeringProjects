import re 


i94addr_dict = dict()
with open('country_code_dictionary.txt','r') as fin:
    for row in fin:
        line = row.split('=')
        pattern = re.compile(r'(\'|\")(.*)(\'|\")')
        key= pattern.search(str(line[0]).strip().strip('\t')).groups()[1]
        val = pattern.search(str(line[1]).strip().strip('\t')).groups()[1]
        if key not in i94addr_dict.keys():
            i94addr_dict[key] = val

i94port_dict = dict()
with open('../data/port_dictionary.txt','r') as fin:
    for row in fin:
        line = row.split('=')
        pattern = re.compile(r'(\'|\")(.*)(\'|\")')
        key= pattern.search(str(line[0]).strip().strip('\t')).groups()[1]
        val = pattern.search(str(line[1])).groups()[1].strip()
        if key not in i94port_dict.keys():
            i94port_dict[key] = val

i94cit_dict = dict()
with open('../data/destination_dictionary.txt','r') as fin:
    for row in fin:
        line = row.split('=')
        pattern = re.compile(r'(\'|\")(.*)(\'|\")')
        key= str(line[0]).strip().strip('\t')
        val = pattern.search(str(line[1]).strip().strip('\t')).groups()[1]
        if key not in i94cit_dict.keys():
            i94cit_dict[key] = val

def valid_data_bycode(dfspark):
    # Import only data with valid 'i94addr', 'i94port', and 'i94cit' 
    df_out = (dfspark[(dfspark['i94addr'].isin(list(i94addr_dict.keys()))) & 
                                 (dfspark['i94port'].isin(list(i94port_dict.keys()))) &
                                 (dfspark['i94cit'].isin(list(i94cit_dict.keys())))]
                  )        
    print('the number of entries with valid port codes: ', df_out.count())
    return df_out