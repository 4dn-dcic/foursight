from collections import OrderedDict

# Created (August 2023) to remove dependency on pandas package in wrangler_checks,
# with ultimate goal of removing pandas (and numpy on which it depends) from Foursight
# package which had grown too large (more than 50MB) for AWS Lambda (doing this should
# reduce Foursight package size by more than 20MB). TODO: Maybe move this to dcicutils.
def convert_table_to_ordered_dict(table: list) -> OrderedDict:
    """
    Given a 2-dimensional array ``table`` which is assumed to have a 0th row that represents the
    column header names for remaining rows in the table, returns a list of ordered dictionaries
    representing the given table rows, where each ordered dictionary contains properties for
    each of the table header column names with values being set to the corresponding values
    for the column from the row. For example, given a table like this:

        [ ['Name',   'Age', 'Location'     ],
          ['Alice',   25,   'New York'     ],
          ['Bob',     30,   'London'       ],
          ['Charlie', 28,   'San Francisco'] ]

     This will be returned:

        [ OrderedDict([('Name', 'Alice'  ), ('Age', 25), ('Location', 'New York'     )]),
          OrderedDict([('Name', 'Bob'    ), ('Age', 30), ('Location', 'London'       )]),
          OrderedDict([('Name', 'Charlie'), ('Age', 28), ('Location', 'San Francisco')]) ]

    """
    header = table[0]
    rows = table[1:]
    # This was originally done using the pandas packages like this:
    # pandas.DataFrame(rows, columns=header).to_dict(orient='records', into=OrderedDict)
    return [OrderedDict(zip(header, row)) for row in rows]
