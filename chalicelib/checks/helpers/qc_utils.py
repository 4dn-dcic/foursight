from dcicutils import ff_utils


def calculate_qc_metric_rnaseq(file_uuid, key):
    '''Patching an RNA-seq gene expression tsv or genome-mapped bam file
    with quality_metric summary'''
    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    qc_summary = []

    for qcs_item in quality_metric.get('quality_metric_summary', []):
        qc_summary.append(qcs_item)
    res = ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return res


def calculate_qc_metric_pairsqc(file_uuid, key):
    '''Patching a pairs file object with quality_metric summary'''
    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    qc_summary = []

    def percent(numVal):
        '''convert to percentage of Total reads'''
        return round((numVal / quality_metric['Total reads']) * 100 * 1000) / 1000

    def million(numVal):
        return str(round(numVal / 10000) / 100) + "m"

    def tooltip(numVal):
        return "Percent of total reads (=%s)" % million(numVal)
    qc_summary.append({"title": "Filtered Reads",
                       "value": str(quality_metric["Total reads"]),
                       "numberType": "integer"})
    qc_summary.append({"title": "Cis reads (>20kb)",
                       "value": str(percent(quality_metric["Cis reads (>20kb)"])),
                       "tooltip": tooltip(quality_metric["Cis reads (>20kb)"]),
                       "numberType": "percent"})
    qc_summary.append({"title": "Short cis reads",
                       "value": str(percent(quality_metric["Short cis reads (<20kb)"])),
                       "tooltip": tooltip(quality_metric["Short cis reads (<20kb)"]),
                       "numberType": "percent"})
    qc_summary.append({"title": "Trans Reads",
                       "value": str(percent(quality_metric["Trans reads"])),
                       "tooltip": tooltip(quality_metric["Trans reads"]),
                       "numberType": "percent"})
    res = ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return res


def calculate_qc_metric_margi_pairsqc(file_uuid, key):
    '''Patching a pairs file object from margi with quality_metric summary'''

    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    qc_summary = []

    def percent_interactions(numVal):
        '''convert to percentage of Total interactions in combined pairs'''
        return round((int(numVal) / int(quality_metric['Total number of interactions'])) * 100 * 1000) / 1000

    def million(numVal):
        return str(round(int(numVal) / 10000) / 100) + "m"

    def tooltip(numVal):
        return "Percent of total interactions (=%s)" % million(int(numVal))

    qc_summary.append({"title": "Filtered Reads",
                       "value": str(quality_metric["Total number of interactions"]),
                       "numberType": "integer"})
    qc_summary.append({"title": "Cis reads (>%s)" % quality_metric["Type"],
                       "value": str(percent_interactions(quality_metric["Distal"])),
                       "tooltip": tooltip(quality_metric["Distal"]),
                       "numberType": "percent"})
    qc_summary.append({"title": "Short cis reads",
                       "value": str(percent_interactions(quality_metric["Proximal"])),
                       "tooltip": tooltip(quality_metric["Proximal"]),
                       "numberType": "percent"})
    qc_summary.append({"title": "Trans Reads",
                       "value": str(percent_interactions(quality_metric["Inter-chromosome interactions"])),
                       "tooltip": tooltip(quality_metric["Inter-chromosome interactions"]),
                       "numberType": "percent"})
    res = ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return res


def parse_formatstr(file_format_str):
    if not file_format_str:
        return None
    return file_format_str.replace('/file-formats/', '').replace('/', '')


def calculate_qc_metric_atacseq_bb(file_uuid, key):
    '''peak call bigbed file from atacseq/chipseq'''
    res = ff_utils.get_metadata(file_uuid, key=key)
    if 'quality_metric' not in res:
        return
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    if 'overlap_reproducibility_qc' not in quality_metric:
        return
    if 'idr_reproducibility_qc' in quality_metric:
        qc_method = 'idr'
    else:
        qc_method = 'overlap'
    qc_summary = []

    def million(numVal):
        return str(round(numVal / 10000) / 100) + "m"

    def tooltip(numVal):
        return "Percent of total reads (=%s)" % million(numVal)

    def round2(numVal):
        return round(numVal * 100) / 100
    opt_set = quality_metric[qc_method + "_reproducibility_qc"]["opt_set"]
    qc_summary.append({"title": "Optimal Peaks",
                       "value": str(quality_metric[qc_method + "_reproducibility_qc"]["N_opt"]),
                       "numberType": "integer"})
    qc_summary.append({"title": "Rescue Ratio",
                       "tooltip": "Ratio of number of peaks (Nt) relative to peak calling based" +
                                  " on psuedoreplicates (Np) [max(Np,Nt) / min (Np,Nt)]",
                       "value": str(round2(quality_metric[qc_method + "_reproducibility_qc"]["rescue_ratio"])),
                       "numberType": "float"})
    qc_summary.append({"title": "Self Consistency Ratio",
                       "tooltip": "Ratio of number of peaks in two replicates [max(N1,N2) / min (N1,N2)]",
                       "value": str(round2(quality_metric[qc_method + "_reproducibility_qc"]["self_consistency_ratio"])),
                       "numberType": "float"})
    qc_summary.append({"title": "Fraction of Reads in Peaks",
                       "value": str(round2(quality_metric[qc_method + "_frip_qc"][opt_set]["FRiP"])),
                       "numberType": "float"})
    ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return qc_summary


def calculate_qc_metric_tagalign(file_uuid, key):
    '''peak call tagAlign bed file from atacseq/chipseq'''
    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    pref = ''
    if 'flagstat_qc' not in quality_metric:
        if 'ctl_flagstat_qc' not in quality_metric:
            return
        else:
            pref = 'ctl_'
    qc_summary = []
    def million(numVal):
        return str(round(numVal / 10000) / 100) + "m"
    def tooltip(numVal):
        return "Percent of total reads (=%s)" % million(numVal)
    def round2(numVal):
        return round(numVal * 100) / 100
    # mitochondrial rate (only for ATAC-seq)
    qc_type = quality_metric['@type'][0]
    if qc_type == 'QualityMetricAtacseq':
        total = quality_metric[pref + "dup_qc"][0]["paired_reads"] + quality_metric[pref + "dup_qc"][0]["unpaired_reads"]
        nonmito = quality_metric[pref + "pbc_qc"][0]["total_read_pairs"]
        mito_rate = round2((1 - (float(nonmito) / float(total))) * 100)
        qc_summary.append({"title": "Percent mitochondrial reads",
                                              "value": str(mito_rate),
                                              "numberType": "percent"})
    qc_summary.append({"title": "Nonredundant Read Fraction (NRF)",
                                          "value": str(round2(quality_metric[pref + "pbc_qc"][0]["NRF"])),
                                          "tooltip": "distinct non-mito read pairs / total non-mito read pairs",
                                          "numberType": "float"})
    qc_summary.append({"title": "PCR Bottleneck Coefficient (PBC)",
                                          "value": str(round2(quality_metric[pref + "pbc_qc"][0]["PBC1"])),
                                          "tooltip": "one-read non-mito read pairs / distinct non-mito read pairs",
                                          "numberType": "float"})
    final_reads = quality_metric[pref + "nodup_flagstat_qc"][0]["read1"]  # PE
    if not final_reads:
        final_reads = quality_metric[pref + "nodup_flagstat_qc"][0]["total"]  # SE
    qc_summary.append({"title": "Filtered & Deduped Reads",
                                          "value": str(final_reads),
                                          "numberType": "integer"})
    ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return qc_summary


def calculate_qc_metric_bamqc(file_uuid, key):
    '''Patching a annotated bam file object with quality_metric summary'''

    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    qc_summary = []

    def total_number_of_reads_per_type(qc):
        unmapped = 0
        multimapped = 0
        duplicates = 0
        walks = 0

        for key, val in qc.items():
            if "N" in key or "X" in key:
                unmapped = unmapped + val
            elif "M" in key and key != "NM":
                multimapped = multimapped + val
            elif "DD" in key:
                duplicates = val
            elif "WW" in key:
                walks = val

        return unmapped, multimapped, duplicates, walks

    unmapped_reads, multi_reads, duplicates, walks = total_number_of_reads_per_type(quality_metric)


    def percent_of_reads(numVal):
        '''convert to percentage of Total reads in bam file'''
        return round((int(numVal) / int(quality_metric['Total Reads'])) * 100 * 1000) / 1000

    def million(numVal):
        return str(round(int(numVal) / 10000) / 100) + "m"

    def tooltip(numVal):
        return "Percent of total reads (=%s)" % million(int(numVal))

    qc_summary.append({"title": "Total Reads",
                       "value": str(quality_metric["Total Reads"]),
                       "numberType": "integer"})
    qc_summary.append({"title": "Unmapped Reads",
                       "value": str(percent_of_reads(unmapped_reads)),
                       "tooltip": tooltip(unmapped_reads),
                       "numberType": "percent"})
    qc_summary.append({"title": "Multimapped Reads",
                       "value": str(percent_of_reads(multi_reads)),
                       "tooltip": tooltip(multi_reads),
                       "numberType": "percent"})
    qc_summary.append({"title": "Duplicate Reads",
                       "value": str(percent_of_reads(duplicates)),
                       "tooltip": tooltip(duplicates),
                       "numberType": "percent"})
    qc_summary.append({"title": "Walks",
                       "value": str(percent_of_reads(walks)),
                       "tooltip": tooltip(walks),
                       "numberType": "percent"})
    qc_summary.append({"title": "Minor Contigs",
                       "value": str(percent_of_reads(quality_metric["Minor Contigs"])),
                       "tooltip": tooltip(quality_metric["Minor Contigs"]),
                       "numberType": "percent"})

    res = ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
