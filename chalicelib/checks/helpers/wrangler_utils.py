import datetime


# calculate strandedness in fastq files
def calculate_rna_strandedness(files):
    '''
    calculates rna strandedness using the beta_actin count in each strand
    args: files = a list of fastq files metadata with the beta-actin count information
    returns: a string either forward, reverse, unstranded or unknown (if paired files are inconsistent)
    '''
    strandedness_info = {}
    problm = False
    for a_file in files:
        if a_file['sense_count'] == 0 and a_file['antisense_count'] == 0:
            problm = True

    if problm:
        strandedness_info['files'] = files
        strandedness_info['calculated_strandedness'] = "zero"
        return strandedness_info

    if len(files) > 1:  # if more than one file
        if files[0]['paired']:
            count_ratio_1 = files[0]['sense_count'] / (files[0]['sense_count'] + files[0]['antisense_count'])
            count_ratio_2 = files[1]['antisense_count'] / (files[1]['sense_count'] + files[1]['antisense_count'])

            if count_ratio_1 > 0.8:
                strandedness_1 = 'forward'
            elif count_ratio_1 < 0.2:
                strandedness_1 = 'reverse'
            else:
                strandedness_1 = 'unstranded'

            if count_ratio_2 > 0.8:
                strandedness_2 = 'forward'
            elif count_ratio_2 < 0.2:
                strandedness_2 = 'reverse'
            else:
                strandedness_2 = 'unstranded'

            if strandedness_1 == strandedness_2:
                strandedness_info['calculated_strandedness'] = strandedness_1
                strandedness_info['files'] = files
            else:
                strandedness_info['calculated_strandedness'] = 'unknown'
                strandedness_info['files'] = files
                strandedness_info['summary'] = files[0]['accession'] + ' is ' + strandedness_1 + ' and ' + files[1]['accession'] + ' is ' + strandedness_2

        else:
            count_ratio_1 = files[0]['sense_count'] / (files[0]['sense_count'] + files[0]['antisense_count'])
            count_ratio_2 = files[1]['sense_count'] / (files[1]['sense_count'] + files[1]['antisense_count'])

            if count_ratio_1 > 0.8:
                strandedness_1 = 'forward'
            elif count_ratio_1 < 0.2:
                strandedness_1 = 'reverse'
            else:
                strandedness_1 = 'unstranded'

            if count_ratio_2 > 0.8:
                strandedness_2 = 'forward'
            elif count_ratio_2 < 0.2:
                strandedness_2 = 'reverse'
            else:
                strandedness_2 = 'unstranded'

            if strandedness_1 == strandedness_2:
                strandedness_info['calculated_strandedness'] = strandedness_1
                strandedness_info['files'] = files
            else:
                strandedness_info['calculated_strandedness'] = 'unknown'
                strandedness_info['files'] = files
                strandedness_info['summary'] = files[0]['accession'] + ' is ' + strandedness_1 +' and ' + files[1]['accession'] + ' is ' + strandedness_2

    else:
        count_ratio = files[0]['sense_count'] / (files[0]['sense_count'] + files[0]['antisense_count'])
        if count_ratio > 0.8:
            final_strandedness = 'forward'
        elif count_ratio < 0.2:
            final_strandedness = 'reverse'
        else:
            final_strandedness = 'unstranded'
        strandedness_info['calculated_strandedness'] = final_strandedness
        strandedness_info['files'] = files

    return strandedness_info


def last_modified_from(days_back):
    """Check if input is a number and return a string to search for items last
    modified from n days ago. Also returns a message with the resulting number.
    """
    try:
        days_back = float(days_back)
    except (ValueError, TypeError):
        from_date_query = ''
        from_text = ''
    else:
        date_now = datetime.datetime.now(datetime.timezone.utc)
        date_diff = datetime.timedelta(days=days_back)
        from_date = datetime.datetime.strftime(date_now - date_diff, "%Y-%m-%d %H:%M")
        from_date_query = '&last_modified.date_modified.from=' + from_date
        from_text = 'modified from %s ' % from_date
    return from_date_query, from_text


def md_cell_maker(item):
    '''Builds a markdown cell'''

    outstr = ""
    if isinstance(item, str):
        outstr = item

    if isinstance(item, set):
        outstr = ",<br>".join(item)

    if isinstance(item, list):
        outstr = "<br>".join([md_cell_maker(i) for i in item])

    if isinstance(item, dict):
        if item.get("link") is None:
            print("Dictionaries in the table should have link fields!\n{}".format(item))
        outstr = "[{}]({})".format(
            item.get("text"),
            item.get("link").replace(")", "%29"))

    if not isinstance(outstr, str):
        print("type(outstr) = " + str(type(outstr)))

    return outstr.replace("'", "\\'")


def md_table_maker(rows, keys, jsx_key, col_widths="[]"):
    '''Builds markdown table'''

    part1 = """
    <MdSortableTable
        key='{}'
        defaultColWidths={{{}}}
    >{{' \\
    """.format(jsx_key, col_widths)

    part2 = ""
    for key in keys:
        part2 += "|" + key
    part2 += "|\\\n" + ("|---" * len(keys)) + "|\\\n"

    part3 = ""
    for row in rows.values():
        row_str = ""
        for key in keys:
            row_str += "|" + md_cell_maker(row.get(key))
        row_str += "|\\\n"
        part3 += row_str

    part4 = "'}</MdSortableTable>"

    return (part1 + part2 + part3 + part4)
