import re
import pprint
import sys

# NOTE:
# All the WHERE ? conditions/constraints in the view definition are chained by
# ANDs. The code is written specifically for that in the interest of time

where_pattern = re.compile('SELECT .* WHERE')
condition_pattern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] \?')
predef_condition_pttern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] [\w\d\']+')
view_name_pattern = re.compile('CREATE VIEW [\w\d]+ AS')
where_in_pattern = re.compile('[\w\.]+ IN \([\w\d\',]+\)')
in_value_pattern = re.compile('\([\w\d\',]+\)')

def build_inverted_index(view_definitions):
    index = dict()
    for view_def in view_definitions:
        # Ignore a comment in queries.sql
        if view_def[0] == "-":
            continue
        view_name = re.findall(view_name_pattern, view_def)[0][12:-3]
        initial_chunk = re.findall(where_pattern, view_def)[0]
        subseq_chunk = re.split(where_pattern, view_def)[1]
        if initial_chunk not in index:
            index[initial_chunk] = dict()
        # Constraints of type Col=?
        constraints = "&".join(re.findall(condition_pattern, subseq_chunk))
        # Constraints of type COl=LITERAL
        predef_constraints = "&".join(re.findall(predef_condition_pttern, subseq_chunk))
        if predef_constraints != "":
            constraints = constraints + "&" +predef_constraints
        index[initial_chunk][constraints] = view_name
    return index

def contains_variable_constraint(constraints):
    for constraint in constraints:
        if "?" in constraint:
            return True
    return False
def semantically_equal(view_constraints, query_constraints):
    view_constraints.sort()
    query_constraints.sort()
    if len(view_constraints) != len(query_constraints):
        return False
    for i in range(len(view_constraints)):
        if " IN " in query_constraints[i]:
            if query_constraints[i].find(view_constraints[i][:-4]) == -1:
                return False
        elif query_constraints[i].find(view_constraints[i][:-1]) == -1:
            return False
    return True

def build_where_clause(view_constraints, query_constraints):
    where_clause = "WHERE "
    view_constraints.sort()
    query_constraints.sort()
    view_constraints_subset = []
    query_constraints_subset = []
    if len(view_constraints) != len(query_constraints):
        return False
    # Separate out constraints with `?`, param values will only be fetched for
    # those
    for i in range(len(view_constraints)):
        if '?' in view_constraints[i]:
            view_constraints_subset.append(view_constraints[i])
            query_constraints_subset.append(query_constraints[i])
    for i in range(len(view_constraints_subset)):
        # everything except `?`
        left_expression = ""
        value = ""
        if " IN " in query_constraints_subset[i]:
            left_expression = view_constraints_subset[i][:-4] + " IN "
            value = re.findall(in_value_pattern, query_constraints_subset[i])[0]
        else:
            left_expression = view_constraints_subset[i][:-1]
            value = re.split(left_expression, query_constraints_subset[i])[1]
        # Remove old table name, Ex users.id
        left_expression = re.split('\.', left_expression)[1]
        where_clause = where_clause + left_expression
        where_clause = where_clause + value
        where_clause = where_clause + " AND "
    # Remove last AND
    where_clause  = where_clause[:-4]
    return where_clause

def get_projection(conditions):
    projection = ""
    for condition in conditions:
        if "?" in condition:
            # Discard ` = ?`
            column = condition[:-4].strip()
            # Discard old column name
            column = re.split('\.', column)[1]
            projection = projection + ", " + column
    # Discard initial ` ,`
    projection = projection[2:]
    return projection

def transform_query(index, query):
    initial_chunk = re.findall(where_pattern, query)[0]
    subseq_chunk = re.split(where_pattern, query)[1]
    if not initial_chunk in index:
        exit("ERROR: unknown query stem {}\n\nQuery was: {}\n\nKnown stems: {}".format(
             initial_chunk, query, index.keys()))
    sub_map = index[initial_chunk]
    query_constraints = re.findall(predef_condition_pttern, subseq_chunk)
    query_constraints = query_constraints + re.findall(where_in_pattern, subseq_chunk)

    if len(sub_map)!=0:
        for key, view_name in sub_map.items():
            if contains_variable_constraint(key.split("&")) == False:
                # View definition does not have any `?`
                projection = get_projection(key.split("&"))
                final_query = "SELECT " + projection + " FROM " + view_name;
                return final_query
            if semantically_equal(key.split("&"), query_constraints):
                projection = get_projection(key.split("&"))
                where_clause = build_where_clause(key.split("&"), query_constraints)
                final_query = "SELECT " + projection + " FROM " + view_name + " " + where_clause
                return final_query
    # If reached here it implies that there was no match
    exit("ERROR: Did not find match for query in the trace file. Query: " + query)

if __name__=="__main__":
    # Read queries.sql and build the inverted index
    queries_file = open(str(sys.argv[1]), "r")
    view_defs = queries_file.read().split("\n")
    queries_file.close()
    if view_defs[-1] == "":
        view_defs = view_defs[:-1]
    index = build_inverted_index(view_defs)
    # Pretty print inverted index
    # pp = pprint.PrettyPrinter(width=41, compact=True)
    # pp.pprint(index)

    # Read queries form the trace file
    trace_file = open(str(sys.argv[2]), "r")
    traces = trace_file.read().split("\n")
    trace_file.close()

    # Transform queries and flush to file
    out_file = open("transformed_trace.sql", "w")
    for trace in traces:
        if trace == "" or trace.startswith("--") or trace.startswith("INSERT") or trace.startswith("REPLACE") or trace.startswith("UPDATE"):
            out_file.write(trace + "\n")
            continue
        tq = transform_query(index, trace)
        #print(tq)
        out_file.write(tq + "\n")
    out_file.close()
