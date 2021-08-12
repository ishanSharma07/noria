import re
import sys
import random

ALL_TABLES = {'users', 'comments', 'hat_requests', 'hates', 'hidden_stories',
              'invitations', 'keystores', 'messages', 'moderations',
              'read_ribbons', 'saved_stories', 'stories', 'tags',
              'suggested_taggings', 'suggested_titles', 'tag_filters',
              'taggings', 'votes', 'invitation_requests'}

# Table -> Set of PKs.
PKs = { table: set() for table in ALL_TABLES }
FKs = {
  'users': [],
  'comments': [(4, 'stories'), (5, 'users')],
  'hat_requests': [(3, 'users')],
  'hates': [(3, 'users'), (4, 'users')],
  'hidden_stories': [(1, 'users'), (2, 'stories')],
  'invitations': [(1, 'users'), (2, 'users')],
  'keystores': [],
  'messages': [(2, 'users'), (3, 'users')],
  'moderations': [(3, 'users'), (4, 'stories'), (5, 'comments'), (6, 'users')],
  'read_ribbons': [(4, 'users'), (5, 'stories')],
  'saved_stories': [(3, 'users'), (4, 'stories')],
  'stories': [(2, 'users')],
  'tags': [],
  'suggested_taggings': [(1, 'stories'), (2, 'tags'), (3, 'users')],
  'suggested_titles': [(1, 'stories'), (2, 'users')],
  'tag_filters': [(3, 'users'), (4, 'tags')],
  'taggings': [(1, 'stories'), (2, 'tags')],
  'votes': [(1, 'users'), (2, 'stories'), (3, 'comments')],
  'invitation_requests': []
}

# Tables at which we discard rows, other tables get rows implicitly discarded
# (the ones associated with discarded rows from these tables).
DISCARD_FROM = { table for table in ALL_TABLES if len(FKs[table]) == 0}
DISCARD_RATIO = 0.05

# Keeps counts of how many rows remain in each table, and how many were discarded
COUNTS = { table: (0, 0) for table in ALL_TABLES }

def find_table(insert):
  insert = insert[len("INSERT INTO "):]
  table = insert[:insert.index(" ")]
  if table[0] == '`' and table[-1] == '`':
      table = table[1:-1]
  return table

def find_value(insert, index):
  values = insert[insert.index("VALUES") + len("VALUES"):].strip()
  values = values[1:-1].strip()
  values = [v.strip() for v in values.split(",")]
  return values[index]

if __name__ == '__main__':
  in_file = open(sys.argv[1], "r")
  out_file = open(sys.argv[2], "w")

  def retain_line(line, table):
    # Update counts.
    COUNTS[table] = (COUNTS[table][0] + 1, COUNTS[table][1])
    # Update PKs mapping.
    PKs[table].add(find_value(line, 0))
    # Write line.
    out_file.write(line)

  def discard_line(line, table):
    COUNTS[table] = (COUNTS[table][0], COUNTS[table][1] + 1)
  
  for line in in_file.readlines():
    if not line.startswith("INSERT"):
      out_file.write(line)
      continue

    table = find_table(line)
    if table in DISCARD_FROM:
      if random.random() > DISCARD_RATIO:
        discard_line(line, table)
      else:
        retain_line(line, table)
    else:
      retain = True
      for fk_index, fk_table in FKs[table]:
        fk_value = find_value(line, fk_index)
        if fk_value == "NULL":
          continue
        if not fk_value in PKs[fk_table]:
          discard_line(line, table)
          retain = False
          break

      if retain:
        retain_line(line, table)

  in_file.close()
  out_file.close()

  # Print info.
  totals = (0, 0)
  for table, pair in COUNTS.items():
    print(table, pair)
    totals = (totals[0] + pair[0], totals[1] + pair[1])
  print('total', totals)
