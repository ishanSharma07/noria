import re

# NOTE:
# All the WHERE ? conditions/constraints in the view definition are chained by
# ANDs. The code is written specifically for that in the interest of time

where_pattern = re.compile('SELECT .* WHERE')
filter_operations = ["=","!=","<", ">", "<=", ">="]
condition_pattern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] \?')
param_condition_pattern = re.compile('[\w\.]+ [=|!=|<|>|<=|>=] [\w\d\']+')
view_name_pattern = re.compile('CREATE VIEW [\w\d]+ AS')

def build_inverted_index(view_definitions):
    index = dict()
    for view_def in view_definitions:
        view_name = re.findall(view_name_pattern, view_def)[0][12:-3]
        initial_chunk = re.findall(where_pattern, view_def)[0]
        if initial_chunk not in index:
            index[initial_chunk] = dict()
        constraints = "&".join(re.findall(condition_pattern, view_def))
        index[initial_chunk][constraints] = view_name
    return index

def semantically_equal(view_constraints, query_constraints):
    view_constraints.sort()
    query_constraints.sort()
    if len(view_constraints) != len(query_constraints):
        return False
    for i in range(len(view_constraints)):
        if query_constraints[i].find(view_constraints[i][:-1]) == -1:
            return False
    return True

def build_where_clause(view_constraints, query_constraints):
    where_clause = "WHERE "
    view_constraints.sort()
    query_constraints.sort()
    for i in range(len(view_constraints)):
        # everything except `?`
        left_expression = view_constraints[i][:-1]
        value = re.split(left_expression, query_constraints[i])[1]
        where_clause = where_clause + left_expression
        where_clause = where_clause + value
        where_clause = where_clause + " AND "
    # Remove last AND
    where_clause  = where_clause[:-4]
    return where_clause

def transform_query(index, query):
    initial_chunk = re.findall(where_pattern, query)[0]
    subseq_chunk = re.split(where_pattern, query)[1]
    sub_map = index[initial_chunk]
    query_constraints = re.findall(param_condition_pattern, subseq_chunk)
    if len(sub_map)!=0:
        for key, view_name in sub_map.items():
            if semantically_equal(key.split("&"), query_constraints):
                where_clause = build_where_clause(key.split("&"), query_constraints)
                final_query = "SELECT * FROM " + view_name + " " + where_clause + ";";
                print(final_query)
            else:
                print("NOPE")

if __name__=="__main__":
    view = "CREATE VIEW q34 AS '\"SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.short_id = ? AND comments.email = ?\"';"
    trace = "CREATE VIEW q34 AS '\"SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.short_id = 5 AND comments.email = 'asdf'\"';"
    index = build_inverted_index([view])
    transform_query(index, trace)
    # print(index)
