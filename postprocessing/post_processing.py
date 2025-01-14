import re
import pprint
import sys

# NOTE:
# All the WHERE ? conditions/constraints in the view definition are chained by
# ANDs. The code is written specifically for that in the interest of time

where_pattern = re.compile('SELECT .* WHERE')
condition_pattern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] \?')
predef_condition_pttern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] [\w\d\':]+')
view_name_pattern = re.compile('CREATE VIEW [\w\d]+ AS')
where_in_pattern = re.compile('[\w\.]+ IN \([\w\d\', ]+\)')
in_value_pattern = re.compile('\([\w\d\', ]+\)')

# Hardcoded views for which our usual pattern matching fails.
# E.g. for the first view (q12) in this dict, we project (upvotes - downvotes)
# in its definition, and use that to ORDER.
hardcoded = {
  'SELECT comments\.\* FROM comments WHERE comments\.story_id = [0-9]+ ' +
  'ORDER BY \(upvotes - downvotes\) < 0 ASC, confidence DESC': 'q12',
}

# Maps a view name to pair (constraints, direct_no_view)
# The first is a string encoding WHERE condition constraints that appear in view
# definition. The second specifies whether or not this view is actually used or
# is skipped and its queries are directly executed against shards.
views_dict = dict()

# Counts how many queries were encountered per view.
counts_dict = dict()

def build_inverted_index(view_definitions):
    index = dict()
    for view_def in view_definitions:
        # Ignore a comment in queries.sql
        if view_def[0] == "-":
            continue
        # If definition starts with @, we do not need to actually use this view,
        # its queries are executed directly against shards.
        direct_no_view = False
        if view_def[0] == "@":
            view_def = view_def[1:]
            direct_no_view = True
        # Logic for chunking view definition into a stem and constraints for
        # pattern lookup.
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
        # Reverse lookup of view based on constraints.
        index[initial_chunk][constraints] = view_name
        views_dict[view_name] = (constraints, direct_no_view)
    return index

def contains_variable_constraint(constraints):
    for constraint in constraints:
        if "?" in constraint:
            return True
    return False
def semantically_equal(view_constraints, query_constraints):
    # Sometimes, our constraint string building logic ends up with a leading '&'
    # which on split results in an empty string. Filter those out.
    view_constraints = [c for c in view_constraints if c != '']
    query_constraints = [c for c in query_constraints if c != '']
    view_constraints.sort()
    query_constraints.sort()

    if len(view_constraints) != len(query_constraints):
        return False
    for i in range(len(view_constraints)):
        # if view definition compares against ?, the actual query that match it
        # replaces ? with a concrete value (or list of values with IN).
        if view_constraints[i][-1] == "?":
            # If the query constraint uses IN, we check that the prefix before
            # ' IN ' in the query, and before ' = ?' in the view match.
            if " IN " in query_constraints[i]:
                if query_constraints[i].find(view_constraints[i][:-4]) == -1:
                    return False
            # No IN, we check that the same view constraint is applied in the
            # query, modulo the replacement of ? with something else.
            elif query_constraints[i].find(view_constraints[i][:-1]) == -1:
                return False
        # View does not use ?, thus it uses a literal or name. This must match
        # exactly in the query.
        elif query_constraints[i] != view_constraints[i]:
            return False
    return True

def build_where_clause(view_constraints, query_constraints):
    # Sometimes, our constraint string building logic ends up with a leading '&'
    # which on split results in an empty string. Filter those out.
    view_constraints = [c for c in view_constraints if c != '']
    query_constraints = [c for c in query_constraints if c != '']
    where_clause = "WHERE "
    view_constraints.sort()
    query_constraints.sort()

    view_constraints_subset = []
    query_constraints_subset = []
    if len(view_constraints) != len(query_constraints):
        return None

    # Separate out constraints with `?`, param values will only be fetched for
    # those
    for i in range(len(view_constraints)):
        if '?' in view_constraints[i]:
            view_constraints_subset.append(view_constraints[i])
            query_constraints_subset.append(query_constraints[i])
    if len(view_constraints_subset) == 0:  # No where clause needed.
        return ""
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
    chunks = re.findall(where_pattern, query)
    if len(chunks) < 1:
        exit("ERROR: could not match query: {}".format(query))
    initial_chunk = chunks[0]
    subseq_chunk = re.split(where_pattern, query)[1]
    if not initial_chunk in index:
        exit("ERROR: unknown query stem {}\n\nQuery: {}\n\nKnown stems: {}".format(
             initial_chunk, query, index.keys()))

    sub_map = index[initial_chunk]
    query_constraints = re.findall(predef_condition_pttern, subseq_chunk)
    query_constraints = query_constraints + re.findall(where_in_pattern, subseq_chunk)

    # This function is called on the view that matches the query definition.
    # It is responsible for turning the query into a query that abides by the
    # syntax limitations of pelton.
    # If the view is not marked with `direct_no_view`, this rewrites the query
    # so that it is executed against the view.
    # If the view is marked with `direct_no_view`, this fixes syntax issues with
    # the query, but leaves its core logic intact so that it is executed against
    # shards and not a view.
    def on_match(view_name, key, direct_no_view):
        counts_dict[view_name] = counts_dict.get(view_name, 0) + 1
        if direct_no_view:
            # This query can be answered directly without views.
            # Queries refer to columns as '<table_name>.<column_name>',
            # we need to remove the <table_name>. prefix to be compatible with
            # pelton.
            return re.sub(r"[A-Za-z_]+\.([A-Za-z_\*]+)", r"\1", query)
        else:
            # This query needs a view. The view is already created.
            # We just need to transform the query so that it is executed against
            # the view.
            where_clause = build_where_clause(key.split("&"), query_constraints)
            if where_clause is None:
                exit("ERROR: could not build where clause for query: " + query)

            final_query = "SELECT * FROM " + view_name + " " + where_clause
            return final_query

    # If the stem of the query matched some view(s) pre-parsed into our
    # reverse index, sub_map would contain all the views matched.
    # Otherwise, it is empty.
    if len(sub_map) != 0:
        # Check if any of the views matched have semantically equivalent
        # constraints (modulo ? substitution).
        for key, view_name in sub_map.items():
            if semantically_equal(key.split("&"), query_constraints):
                _, direct_no_view = views_dict[view_name]
                return on_match(view_name, key, direct_no_view)

    # No views are matched. Perhaps this is one of the queries whose
    # view is changed extensively. Try the hardcoded regex patterns.
    for regex, view_name in hardcoded.items():
        if re.match(regex, query):
            key, direct_no_view = views_dict[view_name]
            return on_match(view_name, key, direct_no_view)

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
        if trace.startswith("#"):
            break
        if trace == "" or trace.startswith("--") or \
           trace.startswith("INSERT") or trace.startswith("REPLACE") or \
           trace.startswith("UPDATE"):
            out_file.write(trace + "\n")
            continue
        tq = transform_query(index, trace)
        out_file.write(tq + "\n")
    out_file.close()

    # Print statistics
    for key, value in counts_dict.items():
      print(key, value)
