# calculate strandedness in fastq files
def calculate_rna_strandedness(files):
    '''
    calculates rna strandedness using the beta_actin count in each strand
    args: files = a list of fastq files metadata with the beta-actin count information
    returns: a string either forward, reverse, unstranded or unknown (if paired files are inconsistent)
    '''

    for a_file in files:
        if a_file['sense_count'] == 0 and a_file['antisense_count'] == 0:
            return "zero"

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
                final_strandedness = strandedness_1
            else:
                final_strandedness = 'unknown'
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
                final_strandedness = strandedness_1
            else:
                final_strandedness = 'unknown'

    else:
        count_ratio = files[0]['sense_count'] / (files[0]['sense_count'] + files[0]['antisense_count'])
        if count_ratio > 0.8:
            final_strandedness = 'forward'
        elif count_ratio < 0.2:
            final_strandedness = 'reverse'
        else:
            final_strandedness = 'unstranded'

    return final_strandedness
